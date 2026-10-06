package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/mcp"
	"github.com/lsegal/aviary/internal/server"
)

func newClientCommand() *cobra.Command {
	command := &cobra.Command{SilenceUsage: true, Use: "client", Short: "Manage local inbound MCP client credentials"}
	var protocols, tools, agents []string
	add := &cobra.Command{SilenceUsage: true, Use: "add <name>", Args: cobra.ExactArgs(1), Short: "Create a scoped client and print its token once", RunE: func(cmd *cobra.Command, args []string) error {
		id, raw, err := config.NewClientCredential()
		if err != nil {
			return err
		}
		clients, err := config.UpdateClients(cfgFile, func(clients []config.ClientConfig) ([]config.ClientConfig, error) {
			return append(clients, config.ClientConfig{ID: id, Name: args[0], TokenHash: config.ClientTokenHash(raw), Protocols: protocols, Tools: tools, Agents: agents}), nil
		})
		return finishClientChange(cmd, clients, raw, err)
	}}
	add.Flags().StringSliceVar(&protocols, "protocols", nil, "Protocol grants (mcp)")
	add.Flags().StringSliceVar(&tools, "tools", nil, "Exact tool grants (agent_run,ping)")
	add.Flags().StringSliceVar(&agents, "agents", nil, "Permitted configured agent names")
	rotate := &cobra.Command{SilenceUsage: true, Use: "rotate <name>", Args: cobra.ExactArgs(1), Short: "Rotate a client's token while preserving its identity", RunE: func(cmd *cobra.Command, args []string) error {
		_, raw, err := config.NewClientCredential()
		if err != nil {
			return err
		}
		clients, err := config.UpdateClients(cfgFile, func(clients []config.ClientConfig) ([]config.ClientConfig, error) {
			for i := range clients {
				if clients[i].Name == args[0] {
					clients[i].TokenHash = config.ClientTokenHash(raw)
					return clients, nil
				}
			}
			return nil, fmt.Errorf("client not found")
		})
		return finishClientChange(cmd, clients, raw, err)
	}}
	remove := &cobra.Command{SilenceUsage: true, Use: "remove <name>", Args: cobra.ExactArgs(1), Short: "Revoke a client and cancel its accepted runs", RunE: func(cmd *cobra.Command, args []string) error {
		clients, err := config.UpdateClients(cfgFile, func(clients []config.ClientConfig) ([]config.ClientConfig, error) {
			for i := range clients {
				if clients[i].Name == args[0] {
					return append(clients[:i], clients[i+1:]...), nil
				}
			}
			return nil, fmt.Errorf("client not found")
		})
		return finishClientChange(cmd, clients, "", err)
	}}
	command.AddCommand(add, rotate, remove)
	return command
}

func finishClientChange(cmd *cobra.Command, clients []config.ClientConfig, raw string, persistErr error) error {
	if persistErr != nil {
		return persistErr
	}
	// Raw tokens go solely to local operator stdout, exactly once after persistence.
	if raw != "" {
		if _, err := fmt.Fprintln(cmd.OutOrStdout(), raw); err != nil {
			return fmt.Errorf("credential persisted but token output failed: %w", err)
		}
	}
	running, _, err := server.IsRunning()
	if err != nil {
		return fmt.Errorf("persisted; unable to determine server status: %w", err)
	}
	if !running {
		_, err = fmt.Fprintln(cmd.ErrOrStderr(), "Client policy persisted for next startup (server offline).")
		return err
	}
	ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
	defer cancel()
	adminToken := token
	if adminToken == "" {
		adminToken, err = server.LoadToken()
		if err != nil {
			return fmt.Errorf("persisted; server installation not acknowledged: %w", err)
		}
	}
	path, err := filepath.Abs(cfgFile)
	if cfgFile == "" {
		path, err = filepath.Abs(config.DefaultPath())
	}
	if err != nil {
		return err
	}
	targetURL := serverURL
	if flag := cmd.Flag("server"); flag == nil || !flag.Changed {
		cfg, err := config.Load(path)
		if err != nil {
			return err
		}
		port := cfg.Server.Port
		if port == 0 {
			port = 16677
		}
		scheme := "https"
		if cfg.Server.NoTLS {
			scheme = "http"
		}
		targetURL = fmt.Sprintf("%s://localhost:%d", scheme, port)
	}
	if err := acknowledgeClientPolicy(ctx, targetURL, adminToken, config.ClientPolicyRevision(clients), path); err != nil {
		return fmt.Errorf("persisted; server installation not acknowledged; revocation is not confirmed: %w", err)
	}
	_, err = fmt.Fprintln(cmd.ErrOrStderr(), "Client policy installed and acknowledged by the running server.")
	return err
}

func acknowledgeClientPolicy(ctx context.Context, baseURL, adminToken, revision, path string) error {
	transport, err := mcp.RemoteHTTPTransport(baseURL)
	if err != nil {
		return err
	}
	defer transport.CloseIdleConnections()
	body, _ := json.Marshal(map[string]string{"revision": revision, "path": path})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/clients/reload", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+adminToken)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("reload request failed")
	}
	defer response.Body.Close() //nolint:errcheck
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("reload returned HTTP %d", response.StatusCode)
	}
	var result struct {
		Revision string `json:"revision"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&result); err != nil || result.Revision != revision {
		return fmt.Errorf("reload acknowledgment did not match persisted policy")
	}
	return nil
}

func init() { rootCmd.AddCommand(newClientCommand()) }

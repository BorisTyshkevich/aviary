package clickhouseconn

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lsegal/aviary/internal/endpointpolicy"
)

type smokeFixture struct {
	Endpoint string `json:"endpoint"`
	Accounts []struct {
		Name     string `json:"name"`
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"accounts"`
}

func livePolicy(raw string) (endpointpolicy.Policy, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return endpointpolicy.Policy{}, fmt.Errorf("invalid endpoint")
	}
	port := 443
	if u.Port() != "" {
		port, err = strconv.Atoi(u.Port())
		if err != nil {
			return endpointpolicy.Policy{}, fmt.Errorf("invalid endpoint")
		}
	}
	ips, err := net.LookupIP(u.Hostname())
	if err != nil || len(ips) == 0 {
		return endpointpolicy.Policy{}, fmt.Errorf("DNS unavailable")
	}
	cidrs := make([]string, 0, len(ips))
	for _, ip := range ips {
		address, ok := netip.AddrFromSlice(ip)
		if !ok {
			continue
		}
		bits := 32
		if address.Is6() {
			bits = 128
		}
		cidrs = append(cidrs, address.String()+fmt.Sprintf("/%d", bits))
	}
	if len(cidrs) == 0 {
		return endpointpolicy.Policy{}, fmt.Errorf("DNS unavailable")
	}
	return endpointpolicy.Policy{Allow: []endpointpolicy.Rule{{Host: u.Hostname(), Ports: []int{port}, CIDRs: cidrs}}}, nil
}

func TestLiveRestrictedAccounts(t *testing.T) {
	path := os.Getenv("AVIARY_CLICKHOUSE_SMOKE_FILE")
	if path == "" {
		t.Skip("AVIARY_CLICKHOUSE_SMOKE_FILE is not set")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatal("smoke credential fixture is unavailable or not owner-only")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("smoke credential fixture cannot be read")
	}
	var fixture smokeFixture
	if err := json.Unmarshal(data, &fixture); err != nil || fixture.Endpoint == "" || len(fixture.Accounts) == 0 {
		t.Fatal("smoke credential fixture is invalid")
	}
	policy, err := livePolicy(fixture.Endpoint)
	if err != nil {
		t.Fatal("smoke policy could not be derived")
	}
	adapter := Adapter{Policy: policy}
	for _, account := range fixture.Accounts {
		t.Run(account.Name, func(t *testing.T) {
			target := Target{Endpoint: fixture.Endpoint, Username: account.Username}
			credential := NewCredentials(account.Password)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			plan, err := policy.Resolve(ctx, fixture.Endpoint, nil)
			if err != nil {
				t.Fatal("policy endpoint resolution failed")
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, plan.URL.String(), strings.NewReader("SELECT displayName(), version(), revision(), timezone() FORMAT JSONEachRow"))
			if err != nil {
				t.Fatal("standard HTTPS request could not be built")
			}
			request.SetBasicAuth(account.Username, account.Password)
			client := &http.Client{Transport: plan.Transport()}
			response, err := client.Do(request)
			client.CloseIdleConnections()
			if err != nil || response.StatusCode != http.StatusOK {
				if response != nil {
					_ = response.Body.Close()
				}
				t.Fatal("policy-bound standard HTTPS request failed")
			}
			_ = response.Body.Close()
			if err := adapter.ValidateReadOnly(ctx, target, credential); err != nil {
				db, openErr := adapter.open(ctx, target, credential)
				if openErr == nil {
					_, openErr = db.QueryContext(ctx, "SELECT 1")
					_ = db.Close()
				}
				t.Fatalf("%v; driver diagnostic: %s", err, redactDiagnostic(openErr, fixture.Endpoint, account.Username, account.Password))
			}
			result, err := adapter.Query(ctx, target, credential, Request{SQL: "SELECT currentUser()", MaxRows: 1, MaxBytes: 1024, Timeout: 10 * time.Second})
			if err != nil || result.QueryID == "" || len(result.Columns) != 1 || len(result.Rows) != 1 || !strings.EqualFold(strings.TrimSpace(result.Rows[0][0].(string)), account.Username) {
				t.Fatal("restricted account identity query failed")
			}
			result, err = adapter.Query(ctx, target, credential, Request{SQL: "SELECT name FROM system.tables LIMIT 1", MaxRows: 1, MaxBytes: 1024, Timeout: 10 * time.Second})
			if err != nil || len(result.Columns) != 1 {
				t.Fatal("schema inspection query failed")
			}
			result, err = adapter.Query(ctx, target, credential, Request{SQL: "SELECT number FROM numbers(10)", MaxRows: 2, MaxBytes: 1024, Timeout: 10 * time.Second})
			if err != nil || len(result.Rows) != 2 || !result.Truncated || result.Columns[0].Type == "" {
				t.Fatal("row-bounded typed result failed")
			}
			result, err = adapter.Query(ctx, target, credential, Request{SQL: "SELECT repeat('x', 4096) AS huge FROM numbers(2)", MaxRows: 2, MaxBytes: 1024, Timeout: 10 * time.Second})
			if err != nil || len(result.Rows) != 0 || !result.Truncated || len(result.Columns) != 1 {
				t.Fatal("byte-bounded result failed")
			}
			for _, statement := range []string{
				"CREATE TEMPORARY TABLE aviary_smoke_forbidden (x UInt8)",
				"SET readonly = 0",
				"GRANT SELECT ON *.* TO default",
				"SELECT 1 SETTINGS max_execution_time = 0",
				"SELECT 1 SETTINGS max_execution_time = 31",
				"SELECT 1 SETTINGS max_memory_usage = 0",
				"SELECT 1 SETTINGS max_result_rows = 0",
				"SELECT 1 SETTINGS max_result_bytes = 0",
			} {
				if _, err := adapter.Query(ctx, target, credential, Request{SQL: statement, MaxRows: 1, MaxBytes: 1024, Timeout: 10 * time.Second}); err == nil {
					t.Fatal("restricted account accepted forbidden operation")
				}
			}
			canceled, stop := context.WithCancel(context.Background())
			stop()
			if _, err := adapter.Query(canceled, target, credential, Request{SQL: "SELECT sleep(3)", MaxRows: 1, MaxBytes: 1024, Timeout: 10 * time.Second}); err == nil {
				t.Fatal("canceled query succeeded")
			}
		})
	}
}

func redactDiagnostic(err error, endpoint, username, password string) string {
	if err == nil {
		return "none"
	}
	text := err.Error()
	for _, value := range []string{endpoint, username, password, url.QueryEscape(password), url.PathEscape(password)} {
		if value != "" {
			text = strings.ReplaceAll(text, value, "<redacted>")
		}
	}
	text = strings.ReplaceAll(text, "Authorization", "<redacted-header>")
	if len(text) > 400 {
		text = text[:400]
	}
	return fmt.Sprintf("%q", text)
}

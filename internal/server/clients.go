package server

import (
	"encoding/json"
	"net/http"
	"path/filepath"

	"github.com/lsegal/aviary/internal/config"
)

// reloadClientsHandler acknowledges the installed disk policy, never a caller's
// supplied grants. It is an administrator endpoint, separate from client MCP.
func (s *Server) reloadClientsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var request struct {
		Revision string `json:"revision"`
		Path     string `json:"path"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&request); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	s.configReloadMu.Lock()
	defer s.configReloadMu.Unlock()
	expectedPath, _ := filepath.Abs(s.configPath)
	requestedPath, _ := filepath.Abs(request.Path)
	if requestedPath != expectedPath {
		http.Error(w, "Configuration path does not match running server", http.StatusConflict)
		return
	}
	cfg, err := config.Load(s.configPath)
	if err != nil {
		http.Error(w, "Client policy could not be loaded", http.StatusConflict)
		return
	}
	if request.Revision != config.ClientPolicyRevision(cfg.Server.Clients) {
		http.Error(w, "Client policy changed; retry with current configuration", http.StatusConflict)
		return
	}
	if err := s.clients.Install(cfg); err != nil {
		http.Error(w, "Invalid client policy", http.StatusBadRequest)
		return
	}
	s.clientMCP.Reconcile()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"revision": s.clients.Revision()})
}

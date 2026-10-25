package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/lsegal/aviary/internal/domain"
	"github.com/lsegal/aviary/internal/store"
)

var ownedSessionMu sync.Mutex
var ownedSessionPattern = regexp.MustCompile(`^mcp_[0-9a-f]{64}$`)

// ClientSession resolves only persisted client-owned MCP conversations. Logical
// names are encoded before hashing; no user string becomes a path component.
func (m *SessionManager) ClientSession(clientID, agentID, name, explicitID string, create bool) (*domain.Session, error) {
	ownedSessionMu.Lock()
	defer ownedSessionMu.Unlock()
	suppliedName := name != ""
	if name == "" {
		name = "main"
	}
	data, _ := json.Marshal([]string{"mcp", clientID, agentID, name})
	sum := sha256.Sum256(data)
	id := "mcp_" + hex.EncodeToString(sum[:])
	if explicitID != "" {
		id = explicitID
	}
	if !ownedSessionPattern.MatchString(id) {
		return nil, errors.New("conversation is not accessible")
	}
	path := store.SessionPath(agentID, id)
	f, err := os.Open(path)
	if err == nil {
		defer f.Close() //nolint:errcheck
		var h sessionRecord
		if err := json.NewDecoder(f).Decode(&h); err != nil {
			return nil, errors.New("conversation is not accessible")
		}
		if h.ID != id || h.ClientID != clientID || h.Protocol != "mcp" || h.OwnerAgentID != agentID {
			return nil, errors.New("conversation is not accessible")
		}
		if explicitID != "" && suppliedName && h.Name != name {
			return nil, errors.New("conversation reference does not match")
		}
		return &domain.Session{ID: h.ID, AgentID: agentID, Name: h.Name, Type: h.Type, ClientID: h.ClientID, Protocol: h.Protocol, CreatedAt: h.CreatedAt, UpdatedAt: h.UpdatedAt}, nil
	}
	if !errors.Is(err, os.ErrNotExist) || explicitID != "" || !create {
		return nil, errors.New("conversation is not accessible")
	}
	sess := &domain.Session{ID: id, AgentID: agentID, Name: name, Type: domain.SessionTypeUser, ClientID: clientID, Protocol: "mcp", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := store.AppendJSONL(path, toSessionRecord(sess)); err != nil {
		return nil, fmt.Errorf("creating client conversation: %w", err)
	}
	return sess, nil
}

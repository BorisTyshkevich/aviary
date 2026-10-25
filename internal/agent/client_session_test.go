package agent

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/store"
)

func TestClientSessionOwnershipAndLogicalNames(t *testing.T) {
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	m := NewSessionManager()
	first, err := m.ClientSession("peer1", "expert", "", "", true)
	require.NoError(t, err)
	same, err := NewSessionManager().ClientSession("peer1", "expert", "", "", true)
	require.NoError(t, err)
	require.Equal(t, first.ID, same.ID)
	explicit, err := m.ClientSession("peer1", "expert", "", first.ID, false)
	require.NoError(t, err)
	require.Equal(t, first.ID, explicit.ID)
	for _, owner := range []string{"peer2", "recreated-peer"} {
		_, err = m.ClientSession(owner, "expert", "", first.ID, true)
		require.Error(t, err)
	}
	for _, id := range []string{"main", "../main", first.ID + "/../main"} {
		_, err = m.ClientSession("peer1", "expert", "", id, true)
		require.Error(t, err)
	}
	ids := map[string]bool{first.ID: true}
	for _, name := range []string{"../main", "a/b", "a_b", " main", "main ", "slack:thread", "A", "a"} {
		s, err := m.ClientSession("peer1", "expert", name, "", true)
		require.NoError(t, err)
		require.False(t, ids[s.ID])
		ids[s.ID] = true
	}
	second, err := m.ClientSession("peer2", "expert", "", "", true)
	require.NoError(t, err)
	require.NotEqual(t, first.ID, second.ID)
	_, err = m.ClientSession("peer1", "expert", "different", first.ID, false)
	require.Error(t, err)
	_, err = os.Stat(store.SessionPath("expert", "main"))
	require.True(t, os.IsNotExist(err))
	// Agent directory sanitization must not serve as an ownership proof.
	a, err := m.ClientSession("peer1", "a:b", "", "", true)
	require.NoError(t, err)
	_, err = m.ClientSession("peer1", "a_b", "", a.ID, false)
	require.Error(t, err)
	entries, err := os.ReadDir(filepath.Join(store.AgentDir("expert"), "sessions"))
	require.NoError(t, err)
	require.Len(t, entries, len(ids)+1)
}

func TestClientSessionDenialDoesNotCreate(t *testing.T) {
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	_, err := NewSessionManager().ClientSession("peer", "expert", "", "mcp_0000000000000000000000000000000000000000000000000000000000000000", true)
	require.Error(t, err)
	_, err = NewSessionManager().ClientSession("peer", "expert", "", "", false)
	require.Error(t, err)
	_, err = os.Stat(store.AgentDir("expert"))
	require.True(t, os.IsNotExist(err))
}

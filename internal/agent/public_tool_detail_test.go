package agent

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPublicToolSQLDetailRedactsLiteralsCommentsAndOtherInputs(t *testing.T) {
	registered := map[string]string{"clickhouse_gen1__query": "clickhouse_gen1__query", "chlab_query": "chlab_query"}
	args := map[string]any{
		"sql":      `SELECT count() FROM events WHERE password = 'fake-secret-password' AND id = 12345 /* fake-secret-comment */ AND url = "fake-secret-url" -- fake-secret-tail`,
		"max_rows": float64(100), "timeout_ms": float64(2500),
		"generation": "fake-secret-generation", "endpoint": "https://fake-secret-host", "token": "fake-secret-token",
	}
	for _, name := range []string{"clickhouse_gen1__query", "chlab_query"} {
		started, ok := projectPublicToolEvent(registered, name, "first", ToolStateStarted, args, time.Second)
		require.True(t, ok)
		require.Zero(t, started.Duration)
		require.Contains(t, started.Detail, "SELECT count() FROM events")
		require.Contains(t, started.Detail, "max_rows=100")
		require.Contains(t, started.Detail, "timeout_ms=2500")
		require.Contains(t, started.Detail, "password = ?")
		require.Contains(t, started.Detail, "id = ?")
		for _, secret := range []string{"fake-secret-password", "fake-secret-comment", "fake-secret-tail", "fake-secret-url", "fake-secret-generation", "fake-secret-host", "fake-secret-token", "12345"} {
			require.NotContains(t, started.Detail, secret)
		}
		finished, ok := projectPublicToolEvent(registered, name, "first", ToolStateSucceeded, args, 3*time.Second)
		require.True(t, ok)
		require.Equal(t, started.Detail, finished.Detail)
		require.Equal(t, 3*time.Second, finished.Duration)
	}
}

func TestPublicToolDetailOnlyShowsTypedSafeScalars(t *testing.T) {
	registered := map[string]string{"web_search": "web_search"}
	args := map[string]any{
		"query": "fake-secret-query", "url": "https://fake-secret-host", "path": "/fake-secret-path",
		"command": "fake-secret-command", "api_key": "fake-secret-key",
		"count": float64(5), "limit": "fake-secret-as-number", "recursive": true,
	}
	first, ok := projectPublicToolEvent(registered, "web_search", "id-a", ToolStateStarted, args, 0)
	require.True(t, ok)
	require.Equal(t, "count=5; recursive=true", first.Detail)
	second, ok := projectPublicToolEvent(registered, "web_search", "id-b", ToolStateFailed, args, time.Second)
	require.True(t, ok)
	require.NotEqual(t, first.InvocationID, second.InvocationID)
	require.Equal(t, first.Detail, second.Detail)
	require.Equal(t, time.Second, second.Duration)
	for _, secret := range []string{"fake-secret-query", "fake-secret-host", "fake-secret-path", "fake-secret-command", "fake-secret-key", "fake-secret-as-number"} {
		require.NotContains(t, first.Detail, secret)
	}
	_, ok = projectPublicToolEvent(registered, "model_fake", "id-c", ToolStateStarted, args, 0)
	require.False(t, ok)
}

func TestPublicToolSQLDetailFailsClosedOnUncertainLexing(t *testing.T) {
	cases := []string{
		"SELECT 'fake-secret-open",
		"SELECT /* fake-secret-open",
		"SELECT /* outer /* fake-secret-nested */ x */ 1",
		"SELECT $fake-secret-dollar$",
		"SELECT {fake-secret-template}",
		"SELECT 1\\fake-secret-escape",
		strings.Repeat("SELECT 1 ", maxPublicSQLInput/9+1),
	}
	for _, sql := range cases {
		detail := publicToolDetail("chlab_query", map[string]any{"sql": sql})
		require.Equal(t, "SQL details unavailable", detail)
		require.NotContains(t, detail, "fake-secret")
	}
}

func TestPublicToolSQLDetailBoundedAfterRedaction(t *testing.T) {
	sql := "SELECT " + strings.Repeat("very_long_identifier, ", 100)
	detail := publicToolDetail("chlab_query", map[string]any{"sql": sql})
	require.LessOrEqual(t, len(detail), len("SQL: ")+maxPublicSQLDetail)
	require.True(t, strings.HasSuffix(detail, "..."))
}

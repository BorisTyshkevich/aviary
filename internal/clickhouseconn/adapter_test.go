package clickhouseconn

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	chblock "github.com/ClickHouse/clickhouse-go/v2/lib/proto"

	"github.com/lsegal/aviary/internal/endpointpolicy"
)

type fixedResolver struct{}

func (fixedResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
}

func nativeResponse(t *testing.T, columns []string, types []column.Type, rows [][]any) []byte {
	t.Helper()
	block := chblock.NewBlock()
	for i, name := range columns {
		if err := block.AddColumn(name, types[i]); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range rows {
		if err := block.Append(row...); err != nil {
			t.Fatal(err)
		}
	}
	buf := new(chproto.Buffer)
	if err := block.Encode(buf, clickhouse.ClientTCPProtocolVersion); err != nil {
		t.Fatal(err)
	}
	return buf.Buf
}

func TestValidateReadOnlyRequiresConnectionModeTwoWithoutInspectingGrants(t *testing.T) {
	for _, tc := range []struct {
		name, readonly string
		readonlyRows   int
		wantOK         bool
		serverReject   bool
	}{
		{name: "mode 2", readonly: "2", readonlyRows: 1, wantOK: true},
		{name: "mode 1", readonly: "1", readonlyRows: 1},
		{name: "mode 0", readonly: "0", readonlyRows: 1},
		{name: "unknown mode", readonly: "3", readonlyRows: 1},
		{name: "malformed mode", readonly: "02", readonlyRows: 1},
		{name: "missing setting", readonly: "2"},
		{name: "truncated setting", readonly: "2", readonlyRows: 2},
		{name: "pinned incompatible setting", serverReject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.Query()["readonly"]; len(got) != 1 || got[0] != "2" {
					t.Errorf("connection did not apply readonly=2: %q", got)
				}
				if got := r.URL.Query().Get("max_execution_time"); got != "" {
					t.Errorf("connection added max_execution_time=%q", got)
				}
				query, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				var body []byte
				switch string(query) {
				case "SELECT displayName(), version(), revision(), timezone()":
					body = nativeResponse(t, []string{"displayName()", "version()", "revision()", "timezone()"}, []column.Type{"String", "String", "UInt32", "String"}, [][]any{{"mock", "25.1.1.1", uint32(clickhouse.ClientTCPProtocolVersion), "UTC"}})
				case "SELECT value FROM system.settings WHERE name = 'readonly'":
					if tc.serverReject {
						w.Header().Set("X-ClickHouse-Exception-Code", "164")
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					rows := make([][]any, tc.readonlyRows)
					for i := range rows {
						rows[i] = []any{tc.readonly}
					}
					body = nativeResponse(t, []string{"value"}, []column.Type{"String"}, rows)
				default:
					t.Errorf("unexpected query: %q", query)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = w.Write(body)
			}))
			defer server.Close()
			certPath := filepath.Join(t.TempDir(), "ca.pem")
			certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
				t.Fatal(err)
			}
			u, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			port, err := strconv.Atoi(u.Port())
			if err != nil {
				t.Fatal(err)
			}
			adapter := Adapter{Policy: endpointpolicy.Policy{Allow: []endpointpolicy.Rule{{Host: u.Hostname(), Ports: []int{port}, CIDRs: []string{"127.0.0.1/32"}}}}, Resolver: fixedResolver{}}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err = adapter.ValidateReadOnly(ctx, Target{Endpoint: server.URL, Username: "reader", CABundlePath: certPath}, NewCredentials("fake-password"))
			if tc.wantOK && err != nil {
				t.Fatalf("readonly=2 connection rejected: %v", err)
			}
			if !tc.wantOK && err == nil {
				t.Fatal("connection without effective readonly=2 accepted")
			}
			if tc.serverReject && !strings.Contains(err.Error(), "server error code 164") {
				t.Fatalf("sanitized ClickHouse rejection code missing: %v", err)
			}
		})
	}
}

func TestCredentialsDoNotMarshalPassword(t *testing.T) {
	encoded, err := json.Marshal(NewCredentials("fake-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "fake-secret") {
		t.Fatal("credential JSON exposed password")
	}
}

func TestRedirectRejectingClosesResponse(t *testing.T) {
	closed := false
	roundTripper := redirectRejecting{host: "db.example", path: "", base: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusFound, Body: closeRecorder{Reader: strings.NewReader("redirect"), closed: &closed}}, nil
	})}
	response, err := roundTripper.RoundTrip(httptest.NewRequest(http.MethodGet, "https://db.example", nil))
	if response != nil || err == nil || !closed {
		t.Fatalf("response=%v err=%v closed=%v", response, err, closed)
	}
}

func TestRedirectRejectingAcceptsOnlyNumericClickHouseExceptionCode(t *testing.T) {
	for _, test := range []struct {
		name, header string
		want         int
	}{
		{name: "positive number", header: "516", want: 516},
		{name: "zero", header: "0"},
		{name: "negative", header: "-1"},
		{name: "overflow", header: "999999999999999999999999999999"},
		{name: "text", header: "Code: 516"},
		{name: "injected text", header: "516\\nprivate server detail"},
	} {
		t.Run(test.name, func(t *testing.T) {
			closed := false
			header := make(http.Header)
			header.Set("X-ClickHouse-Exception-Code", test.header)
			roundTripper := redirectRejecting{host: "db.example", path: "", base: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusBadRequest, Header: header, Body: closeRecorder{Reader: strings.NewReader("private server detail"), closed: &closed}}, nil
			})}
			_, err := roundTripper.RoundTrip(httptest.NewRequest(http.MethodGet, "https://db.example", nil))
			var status httpStatusError
			if !errors.As(err, &status) || status.exceptionCode != test.want || !closed {
				t.Fatalf("exception code=%d want=%d closed=%v", status.exceptionCode, test.want, closed)
			}
			if strings.Contains(err.Error(), "private server detail") {
				t.Fatal("server header detail leaked into error")
			}
		})
	}
}

func TestRedirectRejectingRejectsEndpointEscapeBeforeDial(t *testing.T) {
	called := false
	roundTripper := redirectRejecting{host: "db.example", path: "/clickhouse", base: roundTripFunc(func(*http.Request) (*http.Response, error) { called = true; return nil, nil })}
	request, err := http.NewRequest(http.MethodGet, "https://other.example/clickhouse", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := roundTripper.RoundTrip(request); err == nil || called {
		t.Fatal("endpoint escape reached transport")
	}
}

func TestCredentialsRedactFormatting(t *testing.T) {
	if got := strings.TrimSpace(fmt.Sprint(NewCredentials("fake-secret"))); strings.Contains(got, "fake-secret") {
		t.Fatal("formatted credential exposed password")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type closeRecorder struct {
	io.Reader
	closed *bool
}

func (r closeRecorder) Close() error { *r.closed = true; return nil }

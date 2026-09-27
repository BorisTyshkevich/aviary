package clickhouseconn

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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

func TestSafeGrantsRejectsBroadAccounts(t *testing.T) {
	if safeGrants([][]any{{"GRANT ALL ON *.* TO admin WITH GRANT OPTION"}}) {
		t.Fatal("broad grant was accepted")
	}
	if !safeGrants([][]any{{"GRANT SELECT ON system.* TO reader"}, {"GRANT SHOW ON *.* TO reader"}}) {
		t.Fatal("read-only grants were rejected")
	}
	if safeGrants([][]any{{"GRANT SELECT ON system.* TO reader WITH GRANT OPTION"}}) {
		t.Fatal("grant option was accepted")
	}
	if safeGrants([][]any{{"GRANT SELECT, INSERT ON system.* TO reader"}}) {
		t.Fatal("mixed grant was accepted")
	}
	if safeGrants([][]any{{"GRANT SELECT ON system.*, INSERT ON default.* TO reader"}}) {
		t.Fatal("multi-element grant was accepted")
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

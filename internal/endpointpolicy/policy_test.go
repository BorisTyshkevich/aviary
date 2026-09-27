package endpointpolicy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

type fakeResolver map[string][]netip.Addr

func (r fakeResolver) LookupNetIP(_ context.Context, _ string, host string) ([]netip.Addr, error) {
	return r[host], nil
}

func TestResolveRejectsUnapprovedURLsBeforeDNS(t *testing.T) {
	p := Policy{Allow: []Rule{{Host: "db.example", Ports: []int{8443}, CIDRs: []string{"192.0.2.0/24"}}}}
	for _, raw := range []string{"http://db.example:8443", "https://user:password@db.example:8443", "https://db.example:8443?x=1", "https://db.example:8443?", "https://db.example:8443#x", "https://db.example:8443#", "https://other.example:8443"} {
		if _, err := p.Resolve(context.Background(), raw, fakeResolver{}); err == nil {
			t.Fatalf("Resolve(%q) succeeded", raw)
		}
	}
}

func TestResolvePinsAuthorizedAddressesAndRejectsMixedDNS(t *testing.T) {
	p := Policy{Allow: []Rule{{Host: "db.example", Ports: []int{8443}, CIDRs: []string{"192.0.2.0/24"}}}}
	resolver := fakeResolver{"db.example": {netip.MustParseAddr("192.0.2.7")}}
	plan, err := p.Resolve(context.Background(), "https://db.example:8443/exact/path", resolver)
	if err != nil {
		t.Fatal(err)
	}
	if plan.URL.EscapedPath() != "/exact/path" || plan.ServerName != "db.example" || plan.addresses[0] != "192.0.2.7:8443" {
		t.Fatalf("unexpected plan: %#v", plan)
	}
	resolver["db.example"] = []netip.Addr{netip.MustParseAddr("192.0.2.7"), netip.MustParseAddr("198.51.100.1")}
	if _, err := p.Resolve(context.Background(), "https://db.example:8443", resolver); err == nil {
		t.Fatal("mixed DNS response was accepted")
	}
}

func TestRewriteUsesOnlyAuthorizedRewriteDNSAndPreservesLogicalHost(t *testing.T) {
	var gotHost string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { gotHost = r.Host; w.WriteHeader(http.StatusNoContent) }))
	defer server.Close()
	host, port, _ := strings.Cut(server.Listener.Addr().String(), ":")
	portNumber, _ := strconv.Atoi(port)
	p := Policy{Allow: []Rule{{Host: "example.com", Ports: []int{8443}, CIDRs: []string{"203.0.113.0/24"}}}, Rewrites: []Rewrite{{Host: "example.com", ConnectVia: "proxy.example:" + strconv.Itoa(portNumber), CIDRs: []string{"127.0.0.0/8"}}}}
	plan, err := p.Resolve(context.Background(), "https://example.com:8443/exact", fakeResolver{"proxy.example": {netip.MustParseAddr(host)}})
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	transport := plan.Transport()
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, ServerName: plan.ServerName, MinVersion: tls.VersionTLS12}
	plan.transportFactory = func() *http.Transport { return transport }
	request, err := http.NewRequest(http.MethodGet, plan.URL.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := plan.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if gotHost != "example.com:8443" {
		t.Fatalf("Host = %q", gotHost)
	}

	invalidPlan := plan
	invalidPlan.transportFactory = nil
	invalidTransport := invalidPlan.Transport()
	invalidTransport.TLSClientConfig = &tls.Config{ServerName: invalidPlan.ServerName, MinVersion: tls.VersionTLS12}
	invalidPlan.transportFactory = func() *http.Transport { return invalidTransport }
	invalidRequest, err := http.NewRequest(http.MethodGet, invalidPlan.URL.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := invalidPlan.Do(invalidRequest); err == nil || err.Error() != "endpoint request failed" {
		t.Fatalf("invalid certificate error = %v", err)
	}
}

func TestHTTPClientRejectsRedirect(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://elsewhere.invalid", http.StatusFound)
	}))
	defer server.Close()
	host, port, _ := strings.Cut(server.Listener.Addr().String(), ":")
	portNumber, _ := strconv.Atoi(port)
	p := Policy{Allow: []Rule{{Host: "example.com", Ports: []int{8443}, CIDRs: []string{"127.0.0.0/8"}}}, Rewrites: []Rewrite{{Host: "example.com", ConnectVia: "proxy.example:" + strconv.Itoa(portNumber), CIDRs: []string{"127.0.0.0/8"}}}}
	plan, err := p.Resolve(context.Background(), "https://example.com:8443", fakeResolver{"proxy.example": {netip.MustParseAddr(host)}})
	if err != nil {
		t.Fatal(err)
	}
	requestURL := &url.URL{Scheme: "https", Host: "example.com:8443"}
	plan.URL = requestURL
	transport := plan.Transport()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, ServerName: plan.ServerName, MinVersion: tls.VersionTLS12}
	plan.transportFactory = func() *http.Transport { return transport }
	request, err := http.NewRequest(http.MethodGet, requestURL.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = plan.Do(request)
	if err == nil || err.Error() != "endpoint request failed" || strings.Contains(err.Error(), "elsewhere.invalid") {
		t.Fatalf("redirect error = %v", err)
	}
}

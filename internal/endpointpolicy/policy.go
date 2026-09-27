// Package endpointpolicy validates and dials explicitly authorized outbound
// HTTPS endpoints. It deliberately has no knowledge of a particular protocol.
package endpointpolicy

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
)

// Rule authorizes a logical HTTPS host, port, and each address it resolves to.
// Host is exact or a single left-most wildcard such as *.example.com.
type Rule struct {
	Host  string   `yaml:"host" json:"host"`
	Ports []int    `yaml:"ports" json:"ports"`
	CIDRs []string `yaml:"cidrs" json:"cidrs"`
}

// Rewrite routes a logical host through another TCP destination. CIDRs authorize
// every address resolved for ConnectVia; logical DNS is intentionally not used.
type Rewrite struct {
	Host       string   `yaml:"host" json:"host"`
	ConnectVia string   `yaml:"connect_via" json:"connect_via"`
	CIDRs      []string `yaml:"cidrs" json:"cidrs"`
}

// Policy is fail-closed when no matching logical rule and authorized addresses
// are configured. It is deliberately independent of MCP configuration.
type Policy struct {
	Allow    []Rule    `yaml:"allow,omitempty" json:"allow,omitempty"`
	Rewrites []Rewrite `yaml:"rewrites,omitempty" json:"rewrites,omitempty"`
}

// Resolver is the narrow DNS seam used to make destination authorization
// repeatable in tests. Production uses net.DefaultResolver.
type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

// Plan binds one validated logical URL to authorized concrete IP addresses.
// Dialing an address from the plan prevents a second DNS lookup from changing
// the destination after policy evaluation.
type Plan struct {
	URL              *url.URL
	LogicalHost      string
	ServerName       string
	addresses        []string
	transportFactory func() *http.Transport // test seam
}

// Validate validates policy syntax without performing DNS.
func (p Policy) Validate() error {
	for i, r := range p.Allow {
		if err := validateHostPattern(r.Host); err != nil {
			return fmt.Errorf("allow[%d]: %w", i, err)
		}
		if err := validatePorts(r.Ports); err != nil {
			return fmt.Errorf("allow[%d]: %w", i, err)
		}
		if err := validateCIDRs(r.CIDRs); err != nil {
			return fmt.Errorf("allow[%d]: %w", i, err)
		}
	}
	for i, r := range p.Rewrites {
		if err := validateHostPattern(r.Host); err != nil {
			return fmt.Errorf("rewrites[%d]: %w", i, err)
		}
		_, port, err := net.SplitHostPort(r.ConnectVia)
		if err != nil {
			return fmt.Errorf("rewrites[%d]: connect_via must be host:port", i)
		}
		parsedPort, err := strconv.Atoi(port)
		if err != nil || parsedPort < 1 || parsedPort > 65535 {
			return fmt.Errorf("rewrites[%d]: connect_via port is out of range", i)
		}
		if err := validateCIDRs(r.CIDRs); err != nil {
			return fmt.Errorf("rewrites[%d]: %w", i, err)
		}
	}
	return nil
}

// ValidateURL parses a user endpoint without changing its path. Userinfo,
// query, and fragments are rejected because they can carry credentials or
// driver settings outside the configured connection contract.
func (p Policy) ValidateURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u == nil {
		return nil, fmt.Errorf("endpoint is invalid")
	}
	if u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#") {
		return nil, fmt.Errorf("endpoint is not an allowed HTTPS URL")
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("endpoint is not an allowed HTTPS URL")
	}
	port, err := endpointPort(u)
	if err != nil {
		return nil, fmt.Errorf("endpoint is not an allowed HTTPS URL")
	}
	if !p.logicalAllowed(strings.ToLower(u.Hostname()), port) {
		return nil, fmt.Errorf("endpoint is not permitted")
	}
	return u, nil
}

// Resolve validates raw and returns a single-destination plan. It resolves
// either the rewrite destination or logical host once and rejects if any DNS
// answer falls outside the rule's CIDRs.
func (p Policy) Resolve(ctx context.Context, raw string, resolver Resolver) (Plan, error) {
	u, err := p.ValidateURL(raw)
	if err != nil {
		return Plan{}, err
	}
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	host := strings.ToLower(u.Hostname())
	port, _ := endpointPort(u)
	if rewrite, ok := p.rewriteFor(host); ok {
		dialHost, dialPort, _ := net.SplitHostPort(rewrite.ConnectVia)
		return resolvePlan(ctx, resolver, u, host, dialHost, dialPort, rewrite.CIDRs)
	}
	rule, _ := p.ruleFor(host, port)
	return resolvePlan(ctx, resolver, u, host, host, strconv.Itoa(port), rule.CIDRs)
}

func resolvePlan(ctx context.Context, resolver Resolver, u *url.URL, logicalHost, dialHost, dialPort string, cidrs []string) (Plan, error) {
	ips, err := resolver.LookupNetIP(ctx, "ip", dialHost)
	if err != nil || len(ips) == 0 {
		return Plan{}, fmt.Errorf("endpoint destination cannot be resolved")
	}
	prefixes, _ := parseCIDRs(cidrs)
	addresses := make([]string, 0, len(ips))
	for _, ip := range ips {
		if !allowedIP(ip, prefixes) {
			return Plan{}, fmt.Errorf("endpoint destination is not permitted")
		}
		addresses = append(addresses, net.JoinHostPort(ip.String(), dialPort))
	}
	return Plan{URL: u, LogicalHost: logicalHost, ServerName: logicalHost, addresses: addresses}, nil
}

// Transport returns a proxy-free transport that dials only validated addresses
// while retaining the logical hostname for HTTP Host, TLS SNI and verification.
func (p Plan) Transport() *http.Transport {
	if p.transportFactory != nil {
		return p.transportFactory()
	}
	addresses := append([]string(nil), p.addresses...)
	var next uint64
	return &http.Transport{
		Proxy:           nil,
		TLSClientConfig: &tls.Config{ServerName: p.ServerName, MinVersion: tls.VersionTLS12},
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			if len(addresses) == 0 {
				return nil, fmt.Errorf("endpoint has no authorized destination")
			}
			address := addresses[atomic.AddUint64(&next, 1)%uint64(len(addresses))]
			return (&net.Dialer{}).DialContext(ctx, network, address)
		},
	}
}

// Do executes only the endpoint URL authorized by this plan. Redirects and
// transport failures collapse to stable diagnostics so net/http cannot expose a
// redirect target or request details that may contain sensitive parameters.
func (p Plan) Do(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil || req.URL.String() != p.URL.String() {
		return nil, fmt.Errorf("request does not match the authorized endpoint")
	}
	client := &http.Client{Transport: p.Transport(), CheckRedirect: func(*http.Request, []*http.Request) error { return fmt.Errorf("redirect") }}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		return resp, fmt.Errorf("endpoint request failed")
	}
	return resp, nil
}

func (p Policy) logicalAllowed(host string, port int) bool { _, ok := p.ruleFor(host, port); return ok }
func (p Policy) ruleFor(host string, port int) (Rule, bool) {
	for _, r := range p.Allow {
		if hostMatches(r.Host, host) && portMatches(r.Ports, port) {
			return r, true
		}
	}
	return Rule{}, false
}
func (p Policy) rewriteFor(host string) (Rewrite, bool) {
	for _, r := range p.Rewrites {
		if hostMatches(r.Host, host) {
			return r, true
		}
	}
	return Rewrite{}, false
}
func endpointPort(u *url.URL) (int, error) {
	if u.Port() == "" {
		return 443, nil
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("invalid port")
	}
	return port, nil
}
func portMatches(ports []int, port int) bool {
	for _, p := range ports {
		if p == port {
			return true
		}
	}
	return false
}
func hostMatches(pattern, host string) bool {
	pattern = strings.ToLower(pattern)
	if strings.HasPrefix(pattern, "*.") {
		return strings.HasSuffix(host, pattern[1:]) && host != pattern[2:]
	}
	return host == pattern
}
func validateHostPattern(host string) error {
	if host == "" || strings.ContainsAny(host, "/:@?#[\\]") || (strings.Contains(host, "*") && (!strings.HasPrefix(host, "*.") || strings.Count(host, "*") != 1)) {
		return fmt.Errorf("host must be exact or a left-most wildcard")
	}
	return nil
}
func validatePorts(ports []int) error {
	if len(ports) == 0 {
		return fmt.Errorf("ports is required")
	}
	for _, p := range ports {
		if p < 1 || p > 65535 {
			return fmt.Errorf("port is out of range")
		}
	}
	return nil
}
func validateCIDRs(cidrs []string) error {
	if len(cidrs) == 0 {
		return fmt.Errorf("cidrs is required")
	}
	_, err := parseCIDRs(cidrs)
	return err
}
func parseCIDRs(cidrs []string) ([]netip.Prefix, error) {
	prefixes := make([]netip.Prefix, 0, len(cidrs))
	for _, raw := range cidrs {
		p, err := netip.ParsePrefix(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid CIDR")
		}
		prefixes = append(prefixes, p)
	}
	return prefixes, nil
}
func allowedIP(ip netip.Addr, prefixes []netip.Prefix) bool {
	for _, p := range prefixes {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

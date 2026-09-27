// Package clickhouseconn provides the bounded direct ClickHouse HTTPS adapter.
package clickhouseconn

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/lsegal/aviary/internal/endpointpolicy"
)

var errEndpointRequest = errors.New("endpoint request rejected")

type httpStatusError struct {
	code          int
	exceptionCode int
}

func (e httpStatusError) Error() string { return "HTTP response rejected" }

// Target is trusted connection identity supplied by the connection lifecycle.
// It deliberately carries no credential and must never be populated from model input.
type Target struct{ Endpoint, Username, CABundlePath string }

// Credentials are passed only at invocation. They have no exported fields so
// accidental JSON marshaling and default formatting cannot disclose a password.
type Credentials struct{ password string }

// NewCredentials creates credentials that cannot be accidentally serialized.
func NewCredentials(password string) Credentials { return Credentials{password: password} }

// MarshalJSON prevents credentials from ever entering structured diagnostics.
func (Credentials) MarshalJSON() ([]byte, error) { return json.Marshal(struct{}{}) }

// String redacts credentials from formatting-based diagnostics.
func (Credentials) String() string { return "<redacted>" }

// GoString redacts credentials from Go-syntax diagnostics.
func (Credentials) GoString() string { return "Credentials(<redacted>)" }

// Request contains only caller SQL and resource bounds.
type Request struct {
	SQL               string
	MaxRows, MaxBytes int
	Timeout           time.Duration
}

// Column describes a typed ClickHouse response column.
type Column struct{ Name, Type string }

// Result is a bounded typed query result.
type Result struct {
	Columns   []Column
	Rows      [][]any
	QueryID   string
	Truncated bool
}

// Adapter resolves and pins each HTTPS destination per operation.
type Adapter struct {
	Policy   endpointpolicy.Policy
	Resolver endpointpolicy.Resolver
}

// noDriverDeadline retains cancellation while hiding an operation deadline
// from clickhouse-go. The driver otherwise turns it into max_execution_time,
// which may violate an immutable server-side readonly profile.
type noDriverDeadline struct{ context.Context }

func (noDriverDeadline) Deadline() (time.Time, bool) { return time.Time{}, false }

const (
	maxRows    = 10_000
	maxBytes   = 1 << 20
	maxTimeout = 30 * time.Second
)

// Query executes once. It does not retry, select another target, or accept a
// host, credential, TLS option, or redirect from the request.
func (a Adapter) Query(ctx context.Context, target Target, credential Credentials, request Request) (Result, error) {
	if request.SQL == "" || request.MaxRows < 1 || request.MaxBytes < 1 {
		return Result{}, fmt.Errorf("invalid query request")
	}
	if request.MaxRows > maxRows {
		request.MaxRows = maxRows
	}
	if request.MaxBytes > maxBytes {
		request.MaxBytes = maxBytes
	}
	if request.Timeout <= 0 || request.Timeout > maxTimeout {
		request.Timeout = maxTimeout
	}
	if request.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, request.Timeout)
		defer cancel()
	}
	db, err := a.open(ctx, target, credential)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = db.Close() }()
	queryID, err := newQueryID()
	if err != nil {
		return Result{}, fmt.Errorf("ClickHouse query failed")
	}
	driverContext := clickhouse.Context(noDriverDeadline{Context: ctx}, clickhouse.WithQueryID(queryID))
	rows, err := db.QueryContext(driverContext, request.SQL)
	if err != nil {
		if errors.Is(err, errEndpointRequest) {
			return Result{}, fmt.Errorf("ClickHouse request escaped authorized endpoint")
		}
		var status httpStatusError
		if errors.As(err, &status) {
			if status.exceptionCode > 0 {
				return Result{}, fmt.Errorf("ClickHouse HTTP status %d (server error code %d)", status.code, status.exceptionCode)
			}
			return Result{}, fmt.Errorf("ClickHouse HTTP status %d", status.code)
		}
		var exception *clickhouse.Exception
		if errors.As(err, &exception) {
			return Result{}, fmt.Errorf("ClickHouse server error code %d", exception.Code)
		}
		return Result{}, fmt.Errorf("ClickHouse query failed")
	}
	defer func() { _ = rows.Close() }()
	types, err := rows.ColumnTypes()
	if err != nil {
		return Result{}, fmt.Errorf("ClickHouse query failed")
	}
	result := Result{Columns: make([]Column, len(types)), QueryID: queryID}
	for i, c := range types {
		result.Columns[i] = Column{Name: c.Name(), Type: c.DatabaseTypeName()}
	}
	// Row bytes share a budget with serialized column metadata. Query-ID and
	// JSON envelope punctuation are deliberately outside this data budget.
	encodedColumns, err := json.Marshal(result.Columns)
	if err != nil || len(encodedColumns) > request.MaxBytes {
		return Result{}, fmt.Errorf("ClickHouse result schema exceeds byte limit")
	}
	used := len(encodedColumns)
	for rows.Next() {
		if len(result.Rows) == request.MaxRows {
			result.Truncated = true
			break
		}
		values := make([]any, len(types))
		pointers := make([]any, len(values))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return Result{}, fmt.Errorf("ClickHouse query failed")
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			return Result{}, fmt.Errorf("ClickHouse query failed")
		}
		rowBytes := len(encoded)
		if used+rowBytes > request.MaxBytes {
			result.Truncated = true
			break
		}
		used += rowBytes
		result.Rows = append(result.Rows, values)
	}
	if err := rows.Err(); err != nil {
		return Result{}, fmt.Errorf("ClickHouse query failed")
	}
	return result, nil
}

// ValidateReadOnly confirms the server's readonly setting. Grant inspection is
// intentionally conservative and deployment must provide a least-privilege account.
func (a Adapter) ValidateReadOnly(ctx context.Context, target Target, credential Credentials) error {
	r, err := a.Query(ctx, target, credential, Request{SQL: "SELECT value FROM system.settings WHERE name = 'readonly'", MaxRows: 1, MaxBytes: 1024, Timeout: 10 * time.Second})
	if err != nil {
		return fmt.Errorf("ClickHouse readonly verification query failed")
	}
	if len(r.Rows) != 1 || len(r.Rows[0]) != 1 || fmt.Sprint(r.Rows[0][0]) != "1" {
		return fmt.Errorf("ClickHouse readonly setting is not enforced")
	}
	grants, err := a.Query(ctx, target, credential, Request{SQL: "SHOW GRANTS FINAL", MaxRows: 128, MaxBytes: 32 * 1024, Timeout: 10 * time.Second})
	if err != nil {
		return fmt.Errorf("ClickHouse grant verification query failed")
	}
	if grants.Truncated || !safeGrants(grants.Rows) {
		return fmt.Errorf("ClickHouse account grants are not verified read-only")
	}
	return nil
}

func safeGrants(rows [][]any) bool {
	if len(rows) == 0 {
		return false
	}
	for _, row := range rows {
		if len(row) != 1 {
			return false
		}
		grant := strings.ToUpper(strings.TrimSpace(fmt.Sprint(row[0])))
		if strings.Contains(grant, ",") || strings.Contains(grant, "WITH GRANT OPTION") || (!strings.HasPrefix(grant, "GRANT SELECT ON ") && !strings.HasPrefix(grant, "GRANT SHOW ON ")) {
			return false
		}
	}
	return true
}

func (a Adapter) open(ctx context.Context, target Target, credential Credentials) (*sql.DB, error) {
	plan, err := a.Policy.Resolve(ctx, target.Endpoint, a.Resolver)
	if err != nil {
		return nil, fmt.Errorf("connection endpoint is not permitted")
	}
	transport := plan.Transport()
	if target.CABundlePath != "" {
		pem, err := os.ReadFile(target.CABundlePath)
		if err != nil {
			return nil, fmt.Errorf("configured CA bundle cannot be read")
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("configured CA bundle is invalid")
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, ServerName: plan.ServerName, MinVersion: tls.VersionTLS12}
	}
	path := plan.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	db := clickhouse.OpenDB(&clickhouse.Options{Protocol: clickhouse.HTTP, Addr: []string{plan.URL.Host}, TLS: transport.TLSClientConfig, Auth: clickhouse.Auth{Database: "default", Username: target.Username, Password: credential.password}, HttpUrlPath: path, MaxOpenConns: 1, MaxIdleConns: 0, TransportFunc: func(*http.Transport) (http.RoundTripper, error) {
		return redirectRejecting{base: transport, host: plan.URL.Host, path: path}, nil
	}})
	return db, nil
}

type redirectRejecting struct {
	base       http.RoundTripper
	host, path string
}

func (r redirectRejecting) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Scheme != "https" || request.URL.Host != r.host || request.URL.EscapedPath() != r.path {
		return nil, errEndpointRequest
	}
	response, err := r.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		_ = response.Body.Close()
		return nil, fmt.Errorf("redirect rejected")
	}
	if response.StatusCode >= 400 {
		exceptionCode := clickHouseExceptionCode(response.Header.Get("X-ClickHouse-Exception-Code"))
		_ = response.Body.Close()
		return nil, httpStatusError{code: response.StatusCode, exceptionCode: exceptionCode}
	}
	return response, nil
}

// clickHouseExceptionCode accepts only the stable positive numeric header.
// Server-provided response text and malformed header values are never surfaced.
func clickHouseExceptionCode(value string) int {
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(value, 10, 0)
	if err != nil || parsed <= 0 {
		return 0
	}
	return int(parsed)
}

func newQueryID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("aviary-%x", bytes), nil
}

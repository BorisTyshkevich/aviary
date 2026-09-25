package chlab

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestClusterShapes(t *testing.T) {
	for _, tc := range []struct {
		shape            string
		shards, replicas int
	}{
		{"1x2", 1, 2},
		{"2x2", 2, 4},
	} {
		config := clusterConfig("ch3", tc.shape)
		var parsed struct {
			XMLName xml.Name `xml:"clickhouse"`
		}
		if err := xml.Unmarshal([]byte(config), &parsed); err != nil {
			t.Fatalf("%s XML: %v", tc.shape, err)
		}
		if got := strings.Count(config, "<shard><internal_replication>"); got != tc.shards {
			t.Errorf("%s shards: got %d", tc.shape, got)
		}
		if got := strings.Count(config, "<replica><host>"); got != tc.replicas {
			t.Errorf("%s replicas: got %d", tc.shape, got)
		}
		if !strings.Contains(config, "<macros><shard>02</shard><replica>ch3</replica>") {
			t.Errorf("%s macro mismatch", tc.shape)
		}
		if !strings.Contains(config, "<listen_host>0.0.0.0</listen_host>") {
			t.Errorf("%s peers cannot connect", tc.shape)
		}
	}
}

func TestSessionBindingAndTagValidation(t *testing.T) {
	s := NewService()
	if _, err := s.Call(context.Background(), Request{Action: "status", Agent: "a", Session: "s"}); err == nil {
		t.Fatal("unexpected lab in fresh session")
	}
	for _, tag := range []string{"", "../../image", "25.8;docker", "bad/tag"} {
		if _, err := s.Call(context.Background(), Request{Action: "start", Agent: "a", Session: "s", Version: tag, Shape: "1x1"}); err == nil {
			t.Errorf("accepted unsafe tag %q", tag)
		}
	}
	for _, tag := range []string{"25.8.33.6", "25.8", "latest", "24.8-alpine"} {
		if !tagRE.MatchString(tag) {
			t.Errorf("rejected valid syntax %q", tag)
		}
	}
}

func TestServerSettingsCannotBeRaisedBySQL(t *testing.T) {
	var config struct {
		Profiles struct {
			Default struct {
				MaxExecutionTime int   `xml:"max_execution_time"`
				MaxMemoryUsage   int64 `xml:"max_memory_usage"`
				Constraints      struct {
					MaxExecutionTime struct {
						Readonly string `xml:"readonly"`
					} `xml:"max_execution_time"`
				} `xml:"constraints"`
			} `xml:"default"`
		} `xml:"profiles"`
	}
	if err := xml.Unmarshal([]byte(userConfig), &config); err != nil {
		t.Fatal(err)
	}
	if config.Profiles.Default.MaxExecutionTime != 10 || config.Profiles.Default.MaxMemoryUsage != 536870912 {
		t.Fatal("unexpected SQL resource limits")
	}
	if !strings.Contains(userConfig, "<max_execution_time><readonly/></max_execution_time>") {
		t.Fatal("SQL can override time limit")
	}
}

func TestOutputWriterCapsCopy(t *testing.T) {
	w := &limitWriter{limit: 4}
	n, err := io.Copy(w, struct{ io.Reader }{strings.NewReader("abcdef")})
	if err == nil || n != 4 || w.String() != "abcd" {
		t.Fatalf("copied %d bytes with %q and %v", n, w.String(), err)
	}
}

func TestExpiry(t *testing.T) {
	now := time.Now()
	s := NewService()
	s.labs["active"] = &lab{Status: Status{CreatedAt: now.Add(-10 * time.Minute), LastUsedAt: now}, id: "a"}
	s.labs["idle"] = &lab{Status: Status{CreatedAt: now.Add(-20 * time.Minute), LastUsedAt: now.Add(-16 * time.Minute)}, id: "b"}
	s.labs["total"] = &lab{Status: Status{CreatedAt: now.Add(-61 * time.Minute), LastUsedAt: now}, id: "c"}
	if got := s.expire(now); len(got) != 2 {
		t.Fatalf("expired %d labs, want 2", len(got))
	}
	if len(s.labs) != 1 || s.labs["active"] == nil {
		t.Fatal("active lab was not preserved")
	}
}

func TestStatusNodesSnapshotDuringTransitions(t *testing.T) {
	s := NewService()
	l := &lab{Status: Status{State: "starting", Session: "session", Nodes: map[string]string{"ch1": "starting"}}}
	s.labs[key(Request{Agent: "agent", Session: "session"})] = l

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			l.mu.Lock()
			l.Nodes["ch1"] = "running"
			l.Nodes["ch2"] = "running"
			delete(l.Nodes, "ch2")
			l.mu.Unlock()
		}
	}()
	for i := 0; i < 1000; i++ {
		status, err := s.Call(context.Background(), Request{Action: "status", Agent: "agent", Session: "session"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := json.Marshal(status); err != nil {
			t.Fatal(err)
		}
		if status.Nodes["ch1"] == "" {
			t.Fatal("status lost ch1")
		}
	}
	wg.Wait()
}

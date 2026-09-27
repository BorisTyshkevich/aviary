package scheduler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/domain"
	"github.com/lsegal/aviary/internal/llm"
	"github.com/lsegal/aviary/internal/store"
)

func setupSchedulerDataDir(t *testing.T) {
	t.Helper()
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	err := store.EnsureDirs()
	assert.NoError(t, err)

}

func TestNewIDAndTaskKey(t *testing.T) {
	id := newID("job")
	assert.True(t, strings.HasPrefix(id, "job_"))
	assert.False(t, strings.Contains(id, "."))
	got := taskKey("agentA", "task1")
	assert.Equal(t, "agentA/task1", got)

}

func TestJobQueue_EnqueueClaimCompleteAndList(t *testing.T) {
	setupSchedulerDataDir(t)
	q := NewJobQueue()

	job, err := q.Enqueue("taskA", "a", "hello", "", 0, "", "")
	assert.NoError(t, err)
	assert.Equal(t, domain.JobStatusPending, job.Status)
	assert.Equal(t, defaultRetries, job.MaxRetries)

	claimed, err := q.Claim()
	assert.NoError(t, err)
	assert.NotNil(t, claimed)
	assert.Equal(t, domain.JobStatusInProgress, claimed.Status)
	assert.Equal(t, 1, claimed.Attempts)
	assert.NotNil(t, claimed.LockedAt)
	err = q.Complete(claimed.ID)
	assert.NoError(t, err)

	jobs, err := q.List("taskA")
	assert.NoError(t, err)
	assert.Len(t, jobs, 1)
	assert.Equal(t, domain.JobStatusCompleted, jobs[0].Status)

	all, err := q.List("")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(all))

}

func TestJobQueue_Cancel(t *testing.T) {
	setupSchedulerDataDir(t)
	q := NewJobQueue()

	job, err := q.Enqueue("taskCancel", "a", "hello", "", 0, "", "")
	assert.NoError(t, err)
	err = q.Cancel(job.ID)
	assert.NoError(t, err)

	persisted, err := store.ReadJSON[domain.Job](store.JobPath(job.AgentID, job.ID))
	assert.NoError(t, err)
	assert.Equal(t, domain.JobStatusCanceled, persisted.Status)

}

func TestJobQueue_FailWithRetryThenFailTerminal(t *testing.T) {
	setupSchedulerDataDir(t)
	q := NewJobQueue()

	job, err := q.Enqueue("taskB", "b", "go", "", 2, "", "")
	assert.NoError(t, err)

	first, err := q.Claim()
	assert.NoError(t, err)
	assert.NotNil(t, first)
	err = q.Fail(first.ID, errors.New("boom1"))
	assert.NoError(t, err)

	afterFirst, err := store.ReadJSON[domain.Job](store.JobPath(job.AgentID, job.ID))
	assert.NoError(t, err)
	assert.Equal(t, domain.JobStatusPending, afterFirst.Status)
	assert.NotNil(t, afterFirst.NextRetryAt)

	next := time.Now().Add(-time.Second)
	afterFirst.NextRetryAt = &next
	err = store.WriteJSON(store.JobPath(job.AgentID, job.ID), &afterFirst)
	assert.NoError(t, err)

	second, err := q.Claim()
	assert.NoError(t, err)
	assert.NotNil(t, second)
	err = q.Fail(second.ID, errors.New("boom2"))
	assert.NoError(t, err)

	afterSecond, err := store.ReadJSON[domain.Job](store.JobPath(job.AgentID, job.ID))
	assert.NoError(t, err)
	assert.Equal(t, domain.JobStatusFailed, afterSecond.Status)

}

func TestJobQueue_RecoverStuck(t *testing.T) {
	setupSchedulerDataDir(t)
	q := NewJobQueue()

	job := domain.Job{
		ID:         newID("job"),
		TaskID:     "taskC",
		AgentID:    "c",
		Prompt:     "run",
		Status:     domain.JobStatusInProgress,
		Attempts:   1,
		MaxRetries: 3,
		CreatedAt:  time.Now().Add(-10 * time.Minute),
		UpdatedAt:  time.Now().Add(-10 * time.Minute),
	}
	lockedAt := time.Now().Add(-lockTimeout - time.Second)
	job.LockedAt = &lockedAt
	err := store.WriteJSON(store.JobPath(job.AgentID, job.ID), &job)
	assert.NoError(t, err)

	q.RecoverStuck()

	recovered, err := store.ReadJSON[domain.Job](store.JobPath(job.AgentID, job.ID))
	assert.NoError(t, err)
	assert.Equal(t, domain.JobStatusPending, recovered.Status)
	assert.Nil(t, recovered.LockedAt)

}

func TestJobQueue_RecoverStuck_RecoversRecentInProgress(t *testing.T) {
	setupSchedulerDataDir(t)
	q := NewJobQueue()

	now := time.Now()
	job := domain.Job{
		ID:         newID("job"),
		TaskID:     "taskRecent",
		AgentID:    "recent",
		Prompt:     "run",
		Status:     domain.JobStatusInProgress,
		Attempts:   1,
		MaxRetries: 3,
		CreatedAt:  now,
		UpdatedAt:  now,
		LockedAt:   &now,
	}
	err := store.WriteJSON(store.JobPath(job.AgentID, job.ID), &job)
	assert.NoError(t, err)

	q.RecoverStuck()

	recovered, err := store.ReadJSON[domain.Job](store.JobPath(job.AgentID, job.ID))
	assert.NoError(t, err)
	assert.Equal(t, domain.JobStatusPending, recovered.Status)
	assert.Nil(t, recovered.LockedAt)
}

func TestJobQueue_NewInstanceDoesNotRecoverLiveClaim(t *testing.T) {
	setupSchedulerDataDir(t)
	first := NewJobQueue()
	job, err := first.Enqueue("alpha/daily", "alpha", "run", "", 3, "", "")
	assert.NoError(t, err)
	claimed, err := first.Claim()
	assert.NoError(t, err)
	assert.Equal(t, job.ID, claimed.ID)

	second := NewJobQueue()
	second.RecoverStuck()
	saved, err := store.ReadJSON[domain.Job](store.JobPath(job.AgentID, job.ID))
	assert.NoError(t, err)
	assert.Equal(t, domain.JobStatusInProgress, saved.Status)
	assert.Equal(t, 1, saved.Attempts)
	duplicate, err := second.Claim()
	assert.NoError(t, err)
	assert.Nil(t, duplicate, "a second scheduler must not reclaim a live job")

	assert.NoError(t, first.Fail(job.ID, errors.New("runner stopped")))
	saved, err = store.ReadJSON[domain.Job](store.JobPath(job.AgentID, job.ID))
	assert.NoError(t, err)
	saved.NextRetryAt = nil
	assert.NoError(t, store.WriteJSON(store.JobPath(job.AgentID, job.ID), &saved))
	retried, err := second.Claim()
	assert.NoError(t, err)
	assert.Equal(t, job.ID, retried.ID)
	assert.Equal(t, 2, retried.Attempts)
	assert.NoError(t, second.Cancel(job.ID))
}

func TestWorkerPool_StopClaimsIsAdmissionBarrier(t *testing.T) {
	setupSchedulerDataDir(t)
	queue := NewJobQueue()
	job, err := queue.Enqueue("alpha/daily", "alpha", "run", "", 3, "", "")
	assert.NoError(t, err)
	pool := NewWorkerPool(queue, agent.NewManager(nil), 1)
	pool.StopClaims()
	pool.StopClaims()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)
	pool.Stop()
	saved, err := store.ReadJSON[domain.Job](store.JobPath(job.AgentID, job.ID))
	assert.NoError(t, err)
	assert.Equal(t, domain.JobStatusPending, saved.Status)
	assert.Zero(t, saved.Attempts)
	assert.NoError(t, ctx.Err(), "quiescing claims does not cancel admitted work")
}

func TestScheduler_StopClaimsRejectsManualStarts(t *testing.T) {
	setupSchedulerDataDir(t)
	s, err := New(agent.NewManager(nil), 1)
	assert.NoError(t, err)
	job, err := s.Queue().Enqueue("alpha/daily", "alpha", "run", "", 3, "", "")
	assert.NoError(t, err)
	s.StopClaims()
	_, err = s.RunJobNow(job.ID)
	assert.ErrorContains(t, err, "stopping")
	_, err = s.Trigger("alpha/daily")
	assert.ErrorContains(t, err, "stopping")
	saved, err := store.ReadJSON[domain.Job](store.JobPath(job.AgentID, job.ID))
	assert.NoError(t, err)
	assert.Equal(t, domain.JobStatusPending, saved.Status)
}

func TestJobQueue_RecoverStuck_ExhaustedJobFailsInsteadOfRequeueing(t *testing.T) {
	setupSchedulerDataDir(t)
	q := NewJobQueue()

	now := time.Now()
	job := domain.Job{
		ID:         newID("job"),
		TaskID:     "taskExhausted",
		AgentID:    "exhausted",
		Prompt:     "run",
		Status:     domain.JobStatusInProgress,
		Attempts:   1,
		MaxRetries: 1,
		CreatedAt:  now,
		UpdatedAt:  now,
		LockedAt:   &now,
	}
	err := store.WriteJSON(store.JobPath(job.AgentID, job.ID), &job)
	assert.NoError(t, err)

	q.RecoverStuck()

	recovered, err := store.ReadJSON[domain.Job](store.JobPath(job.AgentID, job.ID))
	assert.NoError(t, err)
	assert.Equal(t, domain.JobStatusFailed, recovered.Status)
	assert.Nil(t, recovered.LockedAt)
}

func TestCronRunner_AddRemove(t *testing.T) {
	r := NewCronRunner()
	called := make(chan struct{}, 1)
	err := r.Add("task", "*/1 * * * * *", func() {
		select {
		case called <- struct{}{}:
		default:
		}
	})
	assert.NoError(t, err)

	_, ok := r.ids["task"]
	assert.True(t, ok)

	r.Start()
	defer r.Stop()

	select {
	case <-called:
	case <-time.After(2500 * time.Millisecond):
		assert.FailNow(t, "timeout")
	}

	r.Remove("task")
	_, ok = r.ids["task"]
	assert.False(t, ok)

}

func TestFileWatcher_AddStartAndRemove(t *testing.T) {
	fw, err := NewFileWatcher()
	assert.NoError(t, err)

	tmp := t.TempDir()
	glob := filepath.Join(tmp, "*.txt")
	triggered := make(chan string, 1)
	err = fw.Add("watch1", glob, func(path string) {
		select {
		case triggered <- path:
		default:
		}
	})
	assert.NoError(t, err)

	_, ok := fw.handlers["watch1"]
	assert.True(t, ok)

	go fw.Start()
	t.Cleanup(fw.Stop)

	path := filepath.Join(tmp, "event.txt")
	err = os.WriteFile(path, []byte("hello"), 0o644)
	assert.NoError(t, err)

	select {
	case got := <-triggered:
		assert.Equal(t, path, got)
	case <-time.After(3 * time.Second):
		assert.FailNow(t, "timeout")
	}

	fw.Remove("watch1")
	_, ok = fw.handlers["watch1"]
	assert.False(t, ok)

}

func TestScheduler_NewStartStopAndReconcile(t *testing.T) {
	setupSchedulerDataDir(t)
	mgr := agent.NewManager(nil)

	s, err := New(mgr, 1)
	assert.NoError(t, err)
	assert.NotNil(t, s.Queue())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	t.Cleanup(s.Stop)

	watchDir := t.TempDir()
	cfg := &config.Config{
		Agents: []config.AgentConfig{
			{
				Name: "alpha",
				Tasks: []config.TaskConfig{
					{Name: "cronTask", Schedule: "*/1 * * * * *", Prompt: "tick"},
					{Name: "watchTask", Watch: filepath.Join(watchDir, "*.md"), Prompt: "watch"},
				},
			},
		},
	}
	s.Reconcile(cfg)
	assert.Equal(t, 2, len(s.tasks))
	_, ok := s.cron.ids["alpha/cronTask"]
	assert.True(t, ok)

	_, ok = s.watch.handlers["alpha/watchTask"]
	assert.True(t, ok)

	s.Reconcile(&config.Config{})
	assert.Len(t, s.tasks, 0)
	assert.Len(t, s.cron.ids, 0)
	assert.Len(t, s.watch.handlers, 0)

}

func TestScheduler_Reconcile_ResolvesRelativeWatchAgainstAgentDir(t *testing.T) {
	setupSchedulerDataDir(t)
	mgr := agent.NewManager(nil)

	s, err := New(mgr, 1)
	assert.NoError(t, err)
	t.Cleanup(s.Stop)

	cfg := &config.Config{
		Agents: []config.AgentConfig{
			{
				Name: "alpha",
				Tasks: []config.TaskConfig{
					{Name: "watchTask", Watch: filepath.Join("docs", "*.md"), Prompt: "watch"},
				},
			},
		},
	}

	s.Reconcile(cfg)

	entry, ok := s.watch.handlers["alpha/watchTask"]
	assert.True(t, ok)
	assert.Equal(t, filepath.Join(store.AgentDir("alpha"), "docs", "*.md"), entry.glob)
	assert.Equal(t, filepath.Join("docs", "*.md"), s.tasks["alpha/watchTask"].Watch)
}

func TestScheduler_RunOnceStartAt_EnqueuesSingleJob(t *testing.T) {
	setupSchedulerDataDir(t)
	mgr := agent.NewManager(nil)

	s, err := New(mgr, 1)
	assert.NoError(t, err)

	t.Cleanup(s.Stop)

	startAt := time.Now().UTC().Add(250 * time.Millisecond).Format(time.RFC3339)
	cfg := &config.Config{
		Agents: []config.AgentConfig{{
			Name: "alpha",
			Tasks: []config.TaskConfig{{
				Name:    "once",
				RunOnce: true,
				StartAt: startAt,
				Prompt:  "ping",
			}},
		}},
	}
	s.Reconcile(cfg)

	time.Sleep(1200 * time.Millisecond)
	jobs, err := s.Queue().List("alpha/once")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))

	time.Sleep(1200 * time.Millisecond)
	jobs, err = s.Queue().List("alpha/once")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))

}

func TestScheduler_DelayedCronStart(t *testing.T) {
	setupSchedulerDataDir(t)
	mgr := agent.NewManager(nil)

	s, err := New(mgr, 1)
	assert.NoError(t, err)

	s.cron.Start()
	t.Cleanup(s.Stop)

	startAt := time.Now().UTC().Add(2 * time.Second).Format(time.RFC3339)
	cfg := &config.Config{
		Agents: []config.AgentConfig{{
			Name: "alpha",
			Tasks: []config.TaskConfig{{
				Name:     "delayed",
				Schedule: "*/1 * * * * *",
				StartAt:  startAt,
				Prompt:   "tick",
			}},
		}},
	}
	s.Reconcile(cfg)

	time.Sleep(1200 * time.Millisecond)
	before, err := s.Queue().List("alpha/delayed")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(before))

	time.Sleep(2200 * time.Millisecond)
	after, err := s.Queue().List("alpha/delayed")
	assert.NoError(t, err)
	assert.NotEqual(t, 0, len(after))

}

func TestScheduler_RunOnceCron_OnlyEnqueuesOneJob(t *testing.T) {
	setupSchedulerDataDir(t)
	mgr := agent.NewManager(nil)

	s, err := New(mgr, 1)
	assert.NoError(t, err)

	s.cron.Start()
	t.Cleanup(s.Stop)

	cfg := &config.Config{
		Agents: []config.AgentConfig{{
			Name: "alpha",
			Tasks: []config.TaskConfig{{
				Name:     "oncecron",
				Schedule: "*/1 * * * * *",
				RunOnce:  true,
				Prompt:   "tick",
			}},
		}},
	}
	s.Reconcile(cfg)

	time.Sleep(3200 * time.Millisecond)
	jobs, err := s.Queue().List("alpha/oncecron")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))

}

func TestScheduler_ListTasksReturnsConfiguredDefinitions(t *testing.T) {
	setupSchedulerDataDir(t)
	mgr := agent.NewManager(nil)

	s, err := New(mgr, 1)
	assert.NoError(t, err)

	t.Cleanup(s.Stop)

	startAt := time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339)
	cfg := &config.Config{
		Agents: []config.AgentConfig{{
			Name: "alpha",
			Tasks: []config.TaskConfig{
				{
					Name:     "daily",
					Schedule: "0 0 10 * * *",
					StartAt:  startAt,
					RunOnce:  true,
					Prompt:   "send report",
					Target:   "slack:alerts:C999",
				},
				{
					Name:   "watch-docs",
					Watch:  "./docs/**/*.md",
					Prompt: "summarize changes",
				},
			},
		}},
	}
	s.Reconcile(cfg)

	tasks := s.ListTasks()
	assert.Equal(t, 2, len(tasks))
	assert.Equal(t, "alpha/daily", tasks[0].ID)
	assert.Equal(t, "alpha", tasks[0].AgentName)
	assert.Equal(t, "daily", tasks[0].Name)
	assert.Equal(t, domain.TriggerTypeCron, tasks[0].TriggerType)
	assert.Equal(t, "0 0 10 * * *", tasks[0].Schedule)
	assert.NotNil(t, tasks[0].StartAt)
	assert.Equal(t, "slack:alerts:C999", tasks[0].Target)
	assert.True(t, tasks[0].RunOnce)
	assert.Equal(t, "alpha/watch-docs", tasks[1].ID)
	assert.Equal(t, domain.TriggerTypeWatch, tasks[1].TriggerType)
	assert.Equal(t, "./docs/**/*.md", tasks[1].Watch)

}

func TestScheduler_ReconcileIgnoresDisabledTasks(t *testing.T) {
	setupSchedulerDataDir(t)
	mgr := agent.NewManager(nil)

	s, err := New(mgr, 1)
	assert.NoError(t, err)

	t.Cleanup(s.Stop)

	disabled := false
	cfg := &config.Config{
		Agents: []config.AgentConfig{{
			Name: "alpha",
			Tasks: []config.TaskConfig{
				{
					Name:     "enabled",
					Schedule: "*/1 * * * * *",
					Prompt:   "run",
				},
				{
					Name:     "disabled",
					Schedule: "*/1 * * * * *",
					Prompt:   "skip",
					Enabled:  &disabled,
				},
			},
		}},
	}
	s.Reconcile(cfg)

	assert.Contains(t, s.tasks, "alpha/enabled")
	assert.NotContains(t, s.tasks, "alpha/disabled")
	assert.Contains(t, s.cron.ids, "alpha/enabled")
	assert.NotContains(t, s.cron.ids, "alpha/disabled")

	tasks := s.ListTasks()
	assert.Len(t, tasks, 1)
	assert.Equal(t, "alpha/enabled", tasks[0].ID)
}

func TestWorkerPool_ExecuteJob(t *testing.T) {
	setupSchedulerDataDir(t)
	mgr := agent.NewManager(nil)
	mgr.Reconcile(&config.Config{Agents: []config.AgentConfig{{Name: "alpha", Model: "m", Tasks: []config.TaskConfig{{Name: "t", Schedule: "*/1 * * * * *", Prompt: "p"}}}}})

	p := NewWorkerPool(NewJobQueue(), mgr, 1)

	err := p.executeJob(context.Background(), &domain.Job{AgentID: "missing", Prompt: "hello"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	err = p.executeJob(context.Background(), &domain.Job{AgentID: "alpha", Prompt: "hello"})
	assert.NoError(t, err)

}

func TestWorkerPool_RejectedRunnerDoesNotWaitForCallback(t *testing.T) {
	setupSchedulerDataDir(t)
	mgr := agent.NewManager(nil)
	mgr.Reconcile(&config.Config{Agents: []config.AgentConfig{{Name: "alpha", Model: "m"}}})
	runner, ok := mgr.Get("alpha")
	assert.True(t, ok)
	runner.Stop()
	p := NewWorkerPool(NewJobQueue(), mgr, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := p.executeJob(ctx, &domain.Job{ID: "job_rejected", AgentID: "alpha", TaskID: "task/alpha", Prompt: "hello"})
	assert.ErrorContains(t, err, "stopping")
	assert.NoError(t, ctx.Err(), "rejected admission left the job waiting for a callback")
}

func TestWorkerPool_RejectedAdmissionDoesNotSpendRetry(t *testing.T) {
	setupSchedulerDataDir(t)
	mgr := agent.NewManager(nil)
	mgr.Reconcile(&config.Config{Agents: []config.AgentConfig{{Name: "alpha", Model: "m"}}})
	runner, ok := mgr.Get("alpha")
	assert.True(t, ok)
	runner.Stop()
	queue := NewJobQueue()
	job, err := queue.Enqueue("task/alpha", "alpha", "hello", "", 1, "", "")
	assert.NoError(t, err)
	claimed, err := queue.Claim()
	assert.NoError(t, err)
	assert.Equal(t, job.ID, claimed.ID)
	assert.Equal(t, 1, claimed.Attempts)
	p := NewWorkerPool(queue, mgr, 1)
	p.processJob(context.Background(), claimed)
	saved, err := store.ReadJSON[domain.Job](store.JobPath("alpha", job.ID))
	assert.NoError(t, err)
	assert.Equal(t, domain.JobStatusPending, saved.Status)
	assert.Zero(t, saved.Attempts)
	assert.NotNil(t, saved.NextRetryAt)
}

func TestWorkerPool_ShutdownCanceledParentRequeuesUnadmittedJob(t *testing.T) {
	setupSchedulerDataDir(t)
	mgr := agent.NewManager(nil)
	mgr.Reconcile(&config.Config{Agents: []config.AgentConfig{{Name: "alpha", Model: "m"}}})
	runner, ok := mgr.Get("alpha")
	assert.True(t, ok)
	runner.Stop()
	queue := NewJobQueue()
	job, err := queue.Enqueue("task/alpha", "alpha", "hello", "", 1, "", "")
	assert.NoError(t, err)
	claimed, err := queue.Claim()
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	NewWorkerPool(queue, mgr, 1).processJob(ctx, claimed)
	saved, err := store.ReadJSON[domain.Job](store.JobPath("alpha", job.ID))
	assert.NoError(t, err)
	assert.Equal(t, domain.JobStatusPending, saved.Status)
	assert.Zero(t, saved.Attempts)
}

func TestWorkerPool_ExplicitStopCancelsUnadmittedJob(t *testing.T) {
	setupSchedulerDataDir(t)
	queue := NewJobQueue()
	job, err := queue.Enqueue("task/alpha", "alpha", "hello", "", 1, "", "")
	assert.NoError(t, err)
	claimed, err := queue.Claim()
	assert.NoError(t, err)
	pool := NewWorkerPool(queue, agent.NewManager(nil), 1)
	pool.registerActiveJob(job.ID, claimed.TaskID, claimed.AgentID, func() {})
	assert.Equal(t, 1, pool.StopJobs(nil))
	pool.finishUnadmittedJob(job.ID)
	saved, err := store.ReadJSON[domain.Job](store.JobPath("alpha", job.ID))
	assert.NoError(t, err)
	assert.Equal(t, domain.JobStatusCanceled, saved.Status)
}

func TestWorkerPool_AcceptedRunnerStopRetainsRetry(t *testing.T) {
	setupSchedulerDataDir(t)
	modelEntered := make(chan struct{}, 1)
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"id\":\"test\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n")
		w.(http.Flusher).Flush()
		modelEntered <- struct{}{}
		<-r.Context().Done()
	}))
	defer model.Close()
	mgr := agent.NewManager(llm.NewFactory(func(string) (string, error) { return "fake-token", nil }).WithProviderOptionsResolver(func(provider string) (llm.ProviderOptions, bool) {
		return llm.ProviderOptions{BaseURI: model.URL}, provider == "vllm"
	}))
	mgr.Reconcile(&config.Config{
		Models: config.ModelsConfig{Providers: map[string]config.ProviderConfig{"vllm": {BaseURI: model.URL}}},
		Agents: []config.AgentConfig{{Name: "alpha", Model: "vllm/test"}},
	})
	runner, ok := mgr.Get("alpha")
	assert.True(t, ok)
	queue := NewJobQueue()
	job, err := queue.Enqueue("alpha/daily", "alpha", "run", "", 2, "", "")
	assert.NoError(t, err)
	claimed, err := queue.Claim()
	assert.NoError(t, err)
	pool := NewWorkerPool(queue, mgr, 1)
	done := make(chan struct{})
	go func() { pool.processJob(context.Background(), claimed); close(done) }()
	select {
	case <-modelEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("accepted model request did not start")
	}
	runner.Stop()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stopped run did not settle its queue job")
	}
	saved, err := store.ReadJSON[domain.Job](store.JobPath(job.AgentID, job.ID))
	assert.NoError(t, err)
	assert.Equal(t, domain.JobStatusPending, saved.Status)
	assert.Equal(t, 1, saved.Attempts)
	assert.NotNil(t, saved.NextRetryAt)
}

func TestWorkerPool_ExecuteJob_RepliesToSessionDelivery(t *testing.T) {
	setupSchedulerDataDir(t)

	mgr := agent.NewManager(nil)
	mgr.Reconcile(&config.Config{Agents: []config.AgentConfig{{Name: "alpha", Model: "m"}}})
	p := NewWorkerPool(NewJobQueue(), mgr, 1)

	const (
		replyAgentID   = "alpha"
		replySessionID = "alpha-signal:+15551234567"
	)

	var delivered string
	agent.RegisterSessionDelivery(replyAgentID, replySessionID, "signal", "+15551234567", func(text string) {
		delivered = text
	})

	job := &domain.Job{
		ID:             "job_reply",
		TaskID:         "oneshot/alpha",
		AgentID:        "alpha",
		Prompt:         "hello",
		ReplyAgentID:   replyAgentID,
		ReplySessionID: replySessionID,
	}
	err := p.executeJob(context.Background(), job)
	assert.NoError(t, err)

	assert.NotEqual(t, "", delivered)

	data, err := os.ReadFile(store.SessionPath(replyAgentID, replySessionID))
	assert.NoError(t, err)
	assert.Contains(t, string(data), "\"role\":\"assistant\"")
	assert.Contains(t, string(data), "Unable to complete this request.")

}

func TestWorkerPool_TaskDeliveryGuardSuppressesNoReply(t *testing.T) {
	assert.False(t, agent.ShouldDeliverReply("NO_REPLY"))
	assert.False(t, agent.ShouldDeliverReply("   "))
	assert.True(t, agent.ShouldDeliverReply("hello"))
}

func TestWorkerPool_ExecuteJob_ReusesPersistedSession(t *testing.T) {
	setupSchedulerDataDir(t)

	mgr := agent.NewManager(nil)
	mgr.Reconcile(&config.Config{Agents: []config.AgentConfig{{Name: "alpha", Model: "m"}}})
	p := NewWorkerPool(NewJobQueue(), mgr, 1)

	sess, err := agent.NewSessionManager().GetOrCreateNamed("alpha", "resume")
	assert.NoError(t, err)

	job := &domain.Job{
		ID:        "job_resume",
		TaskID:    "alpha/daily",
		AgentID:   "alpha",
		SessionID: sess.ID,
		Prompt:    "finish report",
		Attempts:  2,
	}
	err = p.executeJob(context.Background(), job)
	assert.NoError(t, err)

	sessions, err := agent.NewSessionManager().List("alpha")
	assert.NoError(t, err)
	assert.Len(t, sessions, 1)
	assert.Equal(t, sess.ID, sessions[0].ID)

	data, err := os.ReadFile(store.SessionPath("alpha", sess.ID))
	assert.NoError(t, err)
	assert.Contains(t, string(data), "Continue the unfinished scheduled task")
	assert.Contains(t, string(data), "Unable to complete this request.")
}

func TestJobSessionName_UsesStableTaskName(t *testing.T) {
	assert.Equal(t, "daily-report", jobSessionName(&domain.Job{TaskID: "alpha/daily-report"}))
	assert.Equal(t, "follow-up", jobSessionName(&domain.Job{TaskID: "oneshot/follow-up"}))
	assert.Equal(t, "adhoc", jobSessionName(&domain.Job{TaskID: "adhoc"}))
}

func TestWorkerPool_ExecuteJob_UsesStableNamedSession(t *testing.T) {
	setupSchedulerDataDir(t)

	mgr := agent.NewManager(nil)
	mgr.Reconcile(&config.Config{Agents: []config.AgentConfig{{Name: "alpha", Model: "m"}}})
	p := NewWorkerPool(NewJobQueue(), mgr, 1)

	first := &domain.Job{
		ID:      "job_first",
		TaskID:  "alpha/daily-report",
		AgentID: "alpha",
		Prompt:  "first run",
	}
	err := p.executeJob(context.Background(), first)
	assert.NoError(t, err)
	assert.Equal(t, "daily-report", first.SessionID)

	second := &domain.Job{
		ID:      "job_second",
		TaskID:  "alpha/daily-report",
		AgentID: "alpha",
		Prompt:  "second run",
	}
	err = p.executeJob(context.Background(), second)
	assert.NoError(t, err)
	assert.Equal(t, first.SessionID, second.SessionID)

	sessions, err := agent.NewSessionManager().List("alpha")
	assert.NoError(t, err)
	assert.Len(t, sessions, 1)
	assert.Equal(t, "daily-report", sessions[0].Name)
}

func TestWorkerPool_ExecuteJob_UsesBareOneShotTaskIDAsSessionID(t *testing.T) {
	setupSchedulerDataDir(t)

	mgr := agent.NewManager(nil)
	mgr.Reconcile(&config.Config{Agents: []config.AgentConfig{{Name: "alpha", Model: "m"}}})
	p := NewWorkerPool(NewJobQueue(), mgr, 1)

	job := &domain.Job{
		ID:      "job_oneshot",
		TaskID:  "subscript-uptime-check",
		AgentID: "alpha",
		Prompt:  "first run",
	}
	err := p.executeJob(context.Background(), job)
	assert.NoError(t, err)
	assert.Equal(t, "subscript-uptime-check", job.SessionID)
}

func TestExecuteScriptJob(t *testing.T) {
	setupSchedulerDataDir(t)

	job := &domain.Job{
		ID:       "job_script",
		TaskID:   "alpha/scripted",
		TaskType: "script",
		AgentID:  "alpha",
		Script:   testScriptBody(),
	}

	result, err := executeScriptJob(context.Background(), job, &config.AgentConfig{}, nil)
	assert.NoError(t, err)
	assert.Equal(t, "script-ok", result.Output)
	assert.Contains(t, result.Logs, "print: script-ok")
}

func TestCollectJobSessionToolLogs(t *testing.T) {
	setupSchedulerDataDir(t)

	sess, err := agent.NewSessionManager().GetOrCreateNamed("alpha", "logs")
	assert.NoError(t, err)

	before := time.Now().UTC()
	err = agent.AppendMessageToSession("alpha", sess.ID, domain.MessageRoleTool, `{"name":"browser_open","args":{"url":"https://example.com"},"result":"{\"ok\":true}"}`)
	assert.NoError(t, err)
	err = agent.AppendMessageToSession("alpha", sess.ID, domain.MessageRoleAssistant, "final answer")
	assert.NoError(t, err)

	logs, err := collectJobSessionToolLogs("alpha", sess.ID, before.Add(-time.Second), time.Now().UTC().Add(time.Second))
	assert.NoError(t, err)
	assert.Contains(t, logs, "tool [")
	assert.Contains(t, logs, "browser_open")
	assert.Contains(t, logs, "\"url\": \"https://example.com\"")
	assert.Contains(t, logs, "\"ok\": true")
	assert.NotContains(t, logs, "final answer")
}

func testScriptBody() string {
	return "print('script-ok')\n"
}

func TestEnqueueAt(t *testing.T) {
	setupSchedulerDataDir(t)
	q := NewJobQueue()

	at := time.Now().Add(1 * time.Hour)
	job, err := q.EnqueueAt("task1", "alpha", "do something", "", 0, at, "", "")
	assert.NoError(t, err)
	assert.NotNil(t, job.ScheduledFor)
	assert.True(t, job.ScheduledFor.Equal(at))

}

func TestUpdateOutput(t *testing.T) {
	setupSchedulerDataDir(t)
	q := NewJobQueue()

	job, err := q.Enqueue("task1", "alpha", "prompt", "", 0, "", "")
	assert.NoError(t, err)
	err = q.UpdateOutput(job.ID, "some output")
	assert.NoError(t, err)

	// Verify output persisted.
	jobs, err := q.List("")
	assert.NoError(t, err)

	var found *domain.Job
	for i := range jobs {
		if jobs[i].ID == job.ID {
			found = &jobs[i]
		}
	}
	assert.NotNil(t, found)
	assert.Equal(t, "some output", found.Output)

}

func TestUpdateOutput_NotFound(t *testing.T) {
	setupSchedulerDataDir(t)
	q := NewJobQueue()
	err := q.UpdateOutput("nonexistent_job_id", "output")
	assert.Error(t, err)

}

func TestTrigger(t *testing.T) {
	setupSchedulerDataDir(t)

	mgr := agent.NewManager(nil)
	s, err := New(mgr, 1)
	assert.NoError(t, err)

	// Reconcile a config with a task to register it.
	cfg := &config.Config{
		Agents: []config.AgentConfig{
			{
				Name:  "alpha",
				Model: "test/model",
				Tasks: []config.TaskConfig{
					{Name: "daily", Prompt: "run daily", Schedule: "0 9 * * *"},
				},
			},
		},
	}
	mgr.Reconcile(cfg)
	s.Reconcile(cfg)

	job, err := s.Trigger("alpha/daily")
	assert.NoError(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, "alpha/daily", job.TaskID)
	assert.Equal(t, domain.JobStatusInProgress, job.Status)

	deadline := time.Now().Add(2 * time.Second)
	for {
		persisted, readErr := store.ReadJSON[domain.Job](store.JobPath(job.AgentID, job.ID))
		assert.Nil(t, readErr)

		if persisted.Status == domain.JobStatusCompleted {
			break
		}
		assert.False(t, time.Now().After(deadline))

		time.Sleep(25 * time.Millisecond)
	}
}

func TestRunJobNow_ForceStartsPendingJob(t *testing.T) {
	setupSchedulerDataDir(t)

	mgr := agent.NewManager(nil)
	mgr.Reconcile(&config.Config{Agents: []config.AgentConfig{{Name: "alpha", Model: "m"}}})
	s, err := New(mgr, 1)
	assert.NoError(t, err)

	t.Cleanup(s.Stop)

	at := time.Now().Add(1 * time.Hour)
	job, err := s.Queue().EnqueueAt("alpha/daily", "alpha", "hello", "", 1, at, "", "")
	assert.NoError(t, err)

	started, err := s.RunJobNow(job.ID)
	assert.NoError(t, err)
	assert.Equal(t, domain.JobStatusInProgress, started.Status)
	assert.Nil(t, started.ScheduledFor)

}

func TestRunJobNow_RejectsExhaustedPendingJob(t *testing.T) {
	setupSchedulerDataDir(t)

	mgr := agent.NewManager(nil)
	mgr.Reconcile(&config.Config{Agents: []config.AgentConfig{{Name: "alpha", Model: "m"}}})
	s, err := New(mgr, 1)
	assert.NoError(t, err)

	job := domain.Job{
		ID:         newID("job"),
		TaskID:     "alpha/daily",
		AgentID:    "alpha",
		Prompt:     "run",
		Status:     domain.JobStatusPending,
		Attempts:   1,
		MaxRetries: 1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	err = store.WriteJSON(store.JobPath(job.AgentID, job.ID), &job)
	assert.NoError(t, err)

	started, err := s.RunJobNow(job.ID)
	assert.Nil(t, started)
	assert.ErrorContains(t, err, "has exhausted its 1 allowed attempt")
}

func TestTrigger_NotFound(t *testing.T) {
	setupSchedulerDataDir(t)

	mgr := agent.NewManager(nil)
	s, err := New(mgr, 1)
	assert.NoError(t, err)

	_, err = s.Trigger("nonexistent/task")
	assert.Error(t, err)

}

func TestScheduler_StopJobs_CancelsPending(t *testing.T) {
	setupSchedulerDataDir(t)

	mgr := agent.NewManager(nil)
	s, err := New(mgr, 1)
	assert.NoError(t, err)

	job, err := s.Queue().Enqueue("alpha/daily", "alpha", "prompt", "", 1, "", "")
	assert.NoError(t, err)

	stopped, err := s.StopJobs("alpha/daily")
	assert.NoError(t, err)
	assert.Equal(t, 1, stopped)

	persisted, err := store.ReadJSON[domain.Job](store.JobPath(job.AgentID, job.ID))
	assert.NoError(t, err)
	assert.Equal(t, domain.JobStatusCanceled, persisted.Status)

}

func TestJobQueue_ListCorrupted(t *testing.T) {
	setupSchedulerDataDir(t)
	q := NewJobQueue()

	// Enqueue a valid job first.
	job, err := q.Enqueue("task1", "alpha", "prompt", "", 0, "", "")
	assert.NoError(t, err)

	// Write a corrupted file alongside it.
	jobsDir := filepath.Join(store.AgentDir("alpha"), "jobs")
	err = os.WriteFile(filepath.Join(jobsDir, "job_bad.json"), []byte("not-json"), 0o600)
	assert.NoError(t, err)

	// List should skip corrupted file and return the valid job.
	jobs, err := q.List("")
	assert.NoError(t, err)

	found := false
	for _, j := range jobs {
		if j.ID == job.ID {
			found = true
		}
	}
	assert.True(t, found)

}

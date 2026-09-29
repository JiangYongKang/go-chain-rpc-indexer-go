package health

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/types"
)

func testMonitor(t *testing.T) (*Monitor, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	cfg := Config{
		MaxConsecutiveFail: 3,
		ProbeInterval:      5 * time.Millisecond,
		ProbeSuccessNeeded: 2,
		Logger:             log.New(buf, "", 0),
	}
	return NewMonitor([]string{"n1", "n2", "n3"}, cfg), buf
}

func TestEjectionAfterConsecutiveFailures(t *testing.T) {
	m, buf := testMonitor(t)
	// Two failures: healthy -> suspected (logged), then still not ejected.
	if s := m.RecordFailure("n1", types.ErrTimeout); s != StateSuspected {
		t.Fatalf("after 1 failure want suspected, got %s", s)
	}
	if s := m.RecordFailure("n1", types.ErrTimeout); s != StateSuspected {
		t.Fatalf("after 2 failures want suspected, got %s", s)
	}
	// Third failure ejects.
	if s := m.RecordFailure("n1", types.ErrNodeInternal); s != StateEjected {
		t.Fatalf("after 3 failures want ejected, got %s", s)
	}
	st, _ := m.StatusOf("n1")
	if st.ConsecutiveBad != 3 || st.LastCode != types.ErrNodeInternal || st.EjectedAt.IsZero() {
		t.Fatalf("status wrong: %+v", st)
	}
	if got := m.EjectedIDs(); len(got) != 1 || got[0] != "n1" {
		t.Fatalf("ejected=%v want [n1]", got)
	}
	if !strings.Contains(buf.String(), "state healthy -> suspected") ||
		!strings.Contains(buf.String(), "state suspected -> ejected") {
		t.Fatalf("ejection transitions not logged:\n%s", buf.String())
	}

	// A single success elsewhere resets suspicion; ejected n1 is never picked.
	m.RecordSuccess("n2")
	id, ok := m.Pick()
	if !ok || id == "n1" {
		t.Fatalf("pick=%q ok=%v must avoid ejected n1", id, ok)
	}
}

func TestSuccessResetsSuspected(t *testing.T) {
	m, _ := testMonitor(t)
	m.RecordFailure("n2", types.ErrRateLimited)
	if s, _ := m.StatusOf("n2"); s.State != StateSuspected {
		t.Fatalf("want suspected, got %s", s.State)
	}
	m.RecordSuccess("n2")
	if s, _ := m.StatusOf("n2"); s.State != StateHealthy || s.ConsecutiveBad != 0 {
		t.Fatalf("want healthy with zero streak, got %+v", s)
	}
}

func TestPickPrefersHealthierNode(t *testing.T) {
	m, _ := testMonitor(t)
	// n2 has a failure, n1 clean => n1 preferred.
	m.RecordFailure("n2", types.ErrTimeout)
	id, ok := m.Pick()
	if !ok || id != "n1" {
		t.Fatalf("pick=%q want n1 (fewer failures)", id)
	}
	// n1 gets two failures, n2 recovered via success => n2 preferred.
	m.RecordFailure("n1", types.ErrTimeout)
	m.RecordFailure("n1", types.ErrTimeout)
	m.RecordSuccess("n2")
	id, _ = m.Pick()
	if id != "n2" {
		t.Fatalf("pick=%q want n2", id)
	}
}

func TestAllEjectedPickFails(t *testing.T) {
	m, _ := testMonitor(t)
	for _, id := range []string{"n1", "n2", "n3"} {
		for i := 0; i < 3; i++ {
			m.RecordFailure(id, types.ErrNodeInternal)
		}
	}
	if _, ok := m.Pick(); ok {
		t.Fatal("Pick must fail when every node is ejected")
	}
}

func TestProbeRecoveryRequiresStreakAndLogs(t *testing.T) {
	m, buf := testMonitor(t)
	for i := 0; i < 3; i++ {
		m.RecordFailure("n3", types.ErrTimeout)
	}
	now := time.Now()
	if due := m.DueProbes(now.Add(time.Hour)); len(due) != 1 || due[0] != "n3" {
		t.Fatalf("due=%v want [n3]", due)
	}
	// First good probe is not enough (need 2); stays ejected.
	if s := m.MarkProbeResult("n3", true); s != StateEjected {
		t.Fatalf("after 1 probe want ejected, got %s", s)
	}
	// A failed probe resets the streak.
	m.MarkProbeResult("n3", false)
	m.MarkProbeResult("n3", true)
	if s := m.MarkProbeResult("n3", true); s != StateHealthy {
		t.Fatalf("after streak want healthy, got %s", s)
	}
	if got := m.EjectedIDs(); len(got) != 0 {
		t.Fatalf("recovered node still ejected: %v", got)
	}
	logs := buf.String()
	if !strings.Contains(logs, "probe ok=true") || !strings.Contains(logs, "probe ok=false") {
		t.Fatalf("probe results not logged:\n%s", logs)
	}
}

func TestRunProbeLoopRecovers(t *testing.T) {
	buf := &bytes.Buffer{}
	cfg := Config{
		MaxConsecutiveFail: 1,
		ProbeInterval:      5 * time.Millisecond,
		ProbeSuccessNeeded: 1,
		Logger:             log.New(buf, "", 0),
	}
	m := NewMonitor([]string{"n1"}, cfg)
	m.RecordFailure("n1", types.ErrTimeout)
	if got := m.EjectedIDs(); len(got) != 1 {
		t.Fatalf("want ejected, got %v", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	go m.RunProbeLoop(ctx, func(_ context.Context, id string) error {
		calls++
		if calls == 1 {
			return errors.New("still down")
		}
		return nil
	})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(m.EjectedIDs()) == 0 {
			return // recovered
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("node did not recover; logs:\n%s", buf.String())
}

var _ = time.Millisecond

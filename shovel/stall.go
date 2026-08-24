package shovel

import (
	"context"
	"log/slog"
	"sort"
	"time"
)

// stallWatch tracks per-task watermark progress across polls. A task is stalled
// when its watermark has not advanced for at least stallAfter while some other
// task on the same source has advanced since — proof the chain head, the RPC,
// and the database were all healthy while the task sat still. A source-wide
// freeze (an RPC outage) is deliberately not a stall: restarting would not help
// and would loop.
type stallWatch struct {
	stallAfter time.Duration
	seen       map[string]stallSeen // task key -> last distinct watermark
	srcAdvance map[string]time.Time // src -> last time any of its tasks advanced
}

type stallSeen struct {
	num   uint64
	since time.Time
}

type taskTip struct {
	src, ig string
	num     uint64
}

func newStallWatch(stallAfter time.Duration) *stallWatch {
	return &stallWatch{
		stallAfter: stallAfter,
		seen:       map[string]stallSeen{},
		srcAdvance: map[string]time.Time{},
	}
}

// check records the current tips and returns the keys (src/ig) of stalled
// tasks. First sight of a task seeds its state and is not an advance.
func (w *stallWatch) check(now time.Time, tips []taskTip) []string {
	for _, t := range tips {
		k := t.src + "/" + t.ig
		s, ok := w.seen[k]
		if !ok || t.num != s.num {
			w.seen[k] = stallSeen{num: t.num, since: now}
			if ok {
				w.srcAdvance[t.src] = now
			}
		}
	}
	var stalled []string
	for _, t := range tips {
		k := t.src + "/" + t.ig
		s := w.seen[k]
		if now.Sub(s.since) < w.stallAfter {
			continue
		}
		if adv, ok := w.srcAdvance[t.src]; !ok || !adv.After(s.since) {
			continue
		}
		stalled = append(stalled, k)
	}
	sort.Strings(stalled)
	return stalled
}

// WatchStalls polls task watermarks every interval and calls onStall with the
// stalled tasks per [stallWatch.check]. Tasks that are disabled, gate on
// dependencies, or have a configured stop are exempt.
//
// A converge loop can freeze without an error line — its goroutine parked or
// gone — and new-task only happens at process start, so no in-process retry
// can revive it. The intended onStall dumps goroutine stacks and exits,
// letting the supervisor (docker restart policy, systemd) replace the process.
func (tm *Manager) WatchStalls(ctx context.Context, interval, stallAfter time.Duration, onStall func(stalled []string)) {
	w := newStallWatch(stallAfter)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		tips, err := tm.taskTips(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "stall-watch", "error", err)
			continue
		}
		if stalled := w.check(time.Now(), tips); len(stalled) > 0 {
			onStall(stalled)
		}
	}
}

// taskTips returns the current watermark for every stall-eligible task. A task
// with no task_updates rows reports 0, so a task that dies before its first
// write still counts as stalled once its siblings move.
func (tm *Manager) taskTips(ctx context.Context) ([]taskTip, error) {
	igs, err := tm.conf.AllIntegrations(ctx, tm.pgp)
	if err != nil {
		return nil, err
	}
	const q = `
		select src_name, ig_name, max(num)
		from shovel.task_updates
		group by 1, 2
	`
	rows, err := tm.pgp.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	nums := map[string]uint64{}
	for rows.Next() {
		var (
			src, ig string
			num     uint64
		)
		if err := rows.Scan(&src, &ig, &num); err != nil {
			return nil, err
		}
		nums[src+"/"+ig] = num
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var tips []taskTip
	for _, ig := range igs {
		if !ig.Enabled || len(ig.Dependencies) > 0 {
			continue
		}
		for _, src := range ig.Sources {
			if src.Stop > 0 {
				continue
			}
			tips = append(tips, taskTip{src: src.Name, ig: ig.Name, num: nums[src.Name+"/"+ig.Name]})
		}
	}
	return tips, nil
}

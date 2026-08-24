package shovel

import (
	"testing"
	"time"

	"kr.dev/diff"
)

func TestStallWatch(t *testing.T) {
	var (
		t0   = time.Unix(1000, 0)
		tick = func(n int) time.Time { return t0.Add(time.Duration(n) * time.Minute) }
		tips = func(a, b uint64) []taskTip {
			return []taskTip{
				{src: "m", ig: "one", num: a},
				{src: "m", ig: "two", num: b},
			}
		}
		w = newStallWatch(5 * time.Minute)
	)
	// seed. nothing can be stalled on first sight
	diff.Test(t, t.Errorf, w.check(tick(0), tips(10, 10)), []string(nil))
	// both advance every tick. healthy
	for i := 1; i <= 10; i++ {
		diff.Test(t, t.Errorf, w.check(tick(i), tips(10+uint64(i), 10+uint64(i))), []string(nil))
	}
	// "two" freezes at 20 while "one" keeps advancing. below threshold: quiet
	for i := 11; i <= 14; i++ {
		diff.Test(t, t.Errorf, w.check(tick(i), tips(10+uint64(i), 20)), []string(nil))
	}
	// threshold reached and a sibling advanced since the freeze: stalled
	diff.Test(t, t.Errorf, w.check(tick(15), tips(25, 20)), []string{"m/two"})
}

func TestStallWatch_SourceWideFreeze(t *testing.T) {
	var (
		t0   = time.Unix(1000, 0)
		tick = func(n int) time.Time { return t0.Add(time.Duration(n) * time.Minute) }
		w    = newStallWatch(5 * time.Minute)
		tips = []taskTip{
			{src: "m", ig: "one", num: 10},
			{src: "m", ig: "two", num: 10},
		}
	)
	// every task on the source frozen (rpc outage): never stalled
	for i := 0; i <= 30; i++ {
		diff.Test(t, t.Errorf, w.check(tick(i), tips), []string(nil))
	}
}

func TestStallWatch_NeverWrote(t *testing.T) {
	var (
		t0   = time.Unix(1000, 0)
		tick = func(n int) time.Time { return t0.Add(time.Duration(n) * time.Minute) }
		w    = newStallWatch(5 * time.Minute)
		tips = func(a uint64) []taskTip {
			return []taskTip{
				{src: "m", ig: "one", num: a},
				{src: "m", ig: "two", num: 0}, // no task_updates row ever
			}
		}
	)
	diff.Test(t, t.Errorf, w.check(tick(0), tips(10)), []string(nil))
	for i := 1; i <= 4; i++ {
		diff.Test(t, t.Errorf, w.check(tick(i), tips(10+uint64(i))), []string(nil))
	}
	diff.Test(t, t.Errorf, w.check(tick(5), tips(15)), []string{"m/two"})
}

func TestStallWatch_RecoversOnAdvance(t *testing.T) {
	var (
		t0   = time.Unix(1000, 0)
		tick = func(n int) time.Time { return t0.Add(time.Duration(n) * time.Minute) }
		w    = newStallWatch(5 * time.Minute)
		tips = func(a, b uint64) []taskTip {
			return []taskTip{
				{src: "m", ig: "one", num: a},
				{src: "m", ig: "two", num: b},
			}
		}
	)
	diff.Test(t, t.Errorf, w.check(tick(0), tips(10, 10)), []string(nil))
	for i := 1; i <= 4; i++ {
		diff.Test(t, t.Errorf, w.check(tick(i), tips(10+uint64(i), 10)), []string(nil))
	}
	// frozen task catches up right at the threshold: healthy again
	diff.Test(t, t.Errorf, w.check(tick(5), tips(15, 15)), []string(nil))
	diff.Test(t, t.Errorf, w.check(tick(6), tips(16, 16)), []string(nil))
}

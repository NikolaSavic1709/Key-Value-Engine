package tokenBucket

import (
	"time"
)

type TokenBucket struct {
	Interval    time.Duration
	MaxRequests int
	Start time.Time
	AvailableRequests int
}

func (tb *TokenBucket) Handler() bool{
	currentTime := time.Now()
	diff := currentTime.Sub(tb.Start)

	if diff >= tb.Interval {
		tb.AvailableRequests=tb.MaxRequests-1
		tb.Start = time.Now()
		return true
	} else {
		if tb.AvailableRequests == 0 {
			return false
		} else {
			tb.AvailableRequests--
			return true
		}
	}
}

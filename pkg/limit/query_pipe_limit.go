package limit

import (
	"fmt"
	"math"
)

// QueryPipeLimit returns the number of bytes querier can receive from one given source.
// zero or negative == no limit
func QueryPipeLimit() int64 {
	return queryPipeLimit
}

// QueryTotalLimit returns the total number of bytes querier can receive from all sources overall.
// zero or negative == no limit
func QueryTotalLimit() int64 {
	return queryTotalLimit
}

type Limiter interface {
	Add(n int64) error
	Limit() int64
	Current() int64
}

// NewLimiterNoLock creates new limiter which does not use locks.
// Not safe use across several goroutines.
func NewLimiterNoLock(limit int64) Limiter {
	if limit <= 0 {
		return &emptyLimiter{}
	}

	return &limiterNoLock{
		limit:   limit,
		current: 0,
	}
}

// NewLimiterAtomic creates new total query limiter which uses atomic assignments.
// Safe to use across several goroutines.
func NewLimiterAtomic(limit int64) Limiter {
	if limit <= 0 {
		return &emptyLimiter{}
	}

	return &limiterAtomic{
		limit:   limit,
		current: 0,
	}
}

func NewLimiterPropagator(limiter Limiter, parents ...Limiter) Limiter {
	return &limiterPropagator{
		limiter: limiter,
		parents: parents,
	}
}

type limiterPropagator struct {
	limiter Limiter
	parents []Limiter
}

func (l *limiterPropagator) Add(n int64) error {

	if err := l.limiter.Add(n); err != nil {
		return err
	}

	for _, limiter := range l.parents {
		if err := limiter.Add(n); err != nil {
			return err
		}
	}

	return nil
}

func (l *limiterPropagator) Limit() int64 {
	return l.limiter.Limit()
}

func (l *limiterPropagator) Current() int64 {
	return l.limiter.Current()
}


type emptyLimiter struct {
}

func (limiter *emptyLimiter) Add(n int64) error {
	return nil
}

func (limiter *emptyLimiter) Limit() int64 {
	return math.MaxInt64
}

func (limiter *emptyLimiter) Current() int64 {
	return 0
}

type limiterNoLock struct {
	limit   int64
	current int64
}

func (limiter *limiterNoLock) Add(n int64) error {
	limiter.current += n
	if limiter.current > limiter.limit {
		return fmt.Errorf("limit reached: current=%s limit=%s", ByteCountToHuman(limiter.current), LimitToHuman(limiter.limit))
	}

	return nil
}

func (limiter *limiterNoLock) Limit() int64 {
	return limiter.limit
}

func (limiter *limiterNoLock) Current() int64 {
	return limiter.current
}

type limiterAtomic struct {
	limit   int64
	current int64
}

func (limiter *limiterAtomic) Add(n int64) error {
	limiter.current += n
	if limiter.current > limiter.limit {
		return fmt.Errorf("limit reached: current=%s limit=%s", ByteCountToHuman(limiter.current), LimitToHuman(limiter.limit))
	}

	return nil
}

func (limiter *limiterAtomic) Limit() int64 {
	return limiter.limit
}

func (limiter *limiterAtomic) Current() int64 {
	return limiter.current
}

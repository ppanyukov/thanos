package limit

import (
	"fmt"
	"sync/atomic"
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

// NewLimiterNoLock creates new limiter which does not use any locks.
// Not safe use across several goroutines.
func NewLimiterNoLock(limit int64) Limiter {
	if limit <= 0 {
		return &limiterNoop{}
	}

	return &limiterNoLock{
		limit:   limit,
		current: 0,
	}
}

// NewLimiterAtomic creates new limiter which uses atomic increments.
// Safe to use across several goroutines.
func NewLimiterAtomic(limit int64) Limiter {
	if limit <= 0 {
		return &limiterNoop{}
	}

	return &limiterAtomic{
		limit:   limit,
		current: 0,
	}
}

// NewLimiterChain wraps a limiter to propagate counters to zero or more parents.
func NewLimiterChain(limiter Limiter, parents ...Limiter) Limiter {
	return &limiterChain{
		limiter: limiter,
		parents: parents,
	}
}

type limiterChain struct {
	limiter Limiter
	parents []Limiter
}

func (l *limiterChain) Add(n int64) error {
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

func (l *limiterChain) Limit() int64 {
	return l.limiter.Limit()
}

func (l *limiterChain) Current() int64 {
	return l.limiter.Current()
}


type limiterNoop struct {
}

func (limiter *limiterNoop) Add(n int64) error {
	return nil
}

func (limiter *limiterNoop) Limit() int64 {
	return 0
}

func (limiter *limiterNoop) Current() int64 {
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
	current := atomic.AddInt64(&limiter.current, n)
	if current > limiter.limit {
		return fmt.Errorf("limit reached: current=%s limit=%s", ByteCountToHuman(limiter.current), LimitToHuman(limiter.limit))
	}

	return nil
}

func (limiter *limiterAtomic) Limit() int64 {
	return limiter.limit
}

func (limiter *limiterAtomic) Current() int64 {
	return atomic.LoadInt64(&limiter.current)
}

package limit

import (
	"fmt"
)

type QueryPipeLimiter interface {
	AddByteCount(byteCount int) error
}

// NewQueryPipeLimiterNoLock creates new query pipe limiter which does not use locks.
// Not safe use across several goroutines.
func NewQueryPipeLimiterNoLock() QueryPipeLimiter {
	if queryPipeLimit <= 0 {
		return &emptyQueryPipeLimiter{}
	}

	return &queryPipeLimiterNoLock{
		limit:     queryPipeLimit,
		byteCount: 0,
	}
}

type queryPipeLimiterNoLock struct {
	limit     int64
	byteCount int64
}

func (limiter *queryPipeLimiterNoLock) AddByteCount(byteCount int) error {
	limiter.byteCount += int64(byteCount)
	if limiter.byteCount > limiter.limit {
		return fmt.Errorf("limit reached: recieved=%s limit=%s", byteCountToHuman(limiter.byteCount), byteCountToHuman(limiter.limit))
	}

	return nil
}

type emptyQueryPipeLimiter struct {
}

func (limiter *emptyQueryPipeLimiter) AddByteCount(byteCount int) error {
	return nil
}

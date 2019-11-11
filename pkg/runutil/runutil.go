// Package runutil provides helpers to advanced function scheduling control like repeat or retry.
//
// It's very often the case when you need to excutes some code every fixed intervals or have it retried automatically.
// To make it reliably with proper timeout, you need to carefully arrange some boilerplate for this.
// Below function does it for you.
//
// For repeat executes, use Repeat:
//
// 	err := runutil.Repeat(10*time.Second, stopc, func() error {
// 		// ...
// 	})
//
// Retry starts executing closure function f until no error is returned from f:
//
// 	err := runutil.Retry(10*time.Second, stopc, func() error {
// 		// ...
// 	})
//
// For logging an error on each f error, use RetryWithLog:
//
// 	err := runutil.RetryWithLog(logger, 10*time.Second, stopc, func() error {
// 		// ...
// 	})
//
// Another use case for runutil package is when you want to close a `Closer` interface. As we all know, we should close all implements of `Closer`, such as *os.File. Commonly we will use:
//
// 	defer closer.Close()
//
// The problem is that Close() usually can return important error e.g for os.File the actual file flush might happen (and fail) on `Close` method. It's important to *always* check error. Thanos provides utility functions to log every error like those, allowing to put them in convenient `defer`:
//
// 	defer runutil.CloseWithLogOnErr(logger, closer, "log format message")
//
// For capturing error, use CloseWithErrCapture:
//
// 	var err error
// 	defer runutil.CloseWithErrCapture(&err, closer, "log format message")
//
// 	// ...
//
// If Close() returns error, err will capture it and return by argument.
//
// The rununtil.Exhaust* family of functions provide the same functionality but
// they take an io.ReadCloser and they exhaust the whole reader before closing
// them. They are useful when trying to use http keep-alive connections because
// for the same connection to be re-used the whole response body needs to be
// exhausted.
package runutil

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	tsdberrors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/thanos-io/thanos/pkg/limit"
)

func HeapDump(name string) {
	var fName = fmt.Sprintf("/Users/philip/thanos/github.com/ppanyukov/thanos-oom/scripts/%s.pb.gz", name)
	f, _ := os.Create(fName)
	defer f.Close()
	runtime.GC()
	pprof.WriteHeapProfile(f)
	fmt.Printf("WRITTEN HEAP DUMP TO %s\n", fName)
}

// scaleHeapSample adjusts the data from a heap Sample to
// account for its probability of appearing in the collected
// data. heap profiles are a sampling of the memory allocations
// requests in a program. We estimate the unsampled value by dividing
// each collected sample by its probability of appearing in the
// profile. heap profiles rely on a poisson process to determine
// which samples to collect, based on the desired average collection
// rate R. The probability of a sample of size S to appear in that
// profile is 1-exp(-S/R).
func scaleHeapSample(count, size, rate int64) (int64, int64) {
	if count == 0 || size == 0 {
		return 0, 0
	}

	if rate <= 1 {
		// if rate==1 all samples were collected so no adjustment is needed.
		// if rate<1 treat as unknown and skip scaling.
		return count, size
	}

	avgSize := float64(size) / float64(count)
	scale := 1 / (1 - math.Exp(-avgSize/float64(rate)))

	return int64(float64(count) * scale), int64(float64(size) * scale)
}


// copypasta from pprof.WriteHeapProfile.
// we only care for the total.
func getHeapInternal() runtime.MemProfileRecord {
	// Find out how many records there are (MemProfile(nil, true)),
	// allocate that many records, and get the data.
	// There's a race—more records might be added between
	// the two calls—so allocate a few extra records for safety
	// and also try again if we're very unlucky.
	// The loop should only execute one iteration in the common case.
	var p []runtime.MemProfileRecord
	n, ok := runtime.MemProfile(nil, true)
	for {
		// Allocate room for a slightly bigger profile,
		// in case a few more entries have been added
		// since the call to MemProfile.
		p = make([]runtime.MemProfileRecord, n+50)
		n, ok = runtime.MemProfile(p, true)
		if ok {
			p = p[0:n]
			break
		}
		// Profile grew; try again.
	}

	sort.Slice(p, func(i, j int) bool { return p[i].InUseBytes() > p[j].InUseBytes() })

	var total runtime.MemProfileRecord
	for i := range p {
		r := &p[i]
		total.AllocBytes += r.AllocBytes
		total.AllocObjects += r.AllocObjects
		total.FreeBytes += r.FreeBytes
		total.FreeObjects += r.FreeObjects
	}

	return total
}

//func getHeapDelta(before runtime.MemProfileRecord, after runtime.MemProfileRecord) runtime.MemProfileRecord {
//	return runtime.MemProfileRecord{
//		AllocBytes:   after.AllocBytes - before.AllocBytes,
//		FreeBytes:    after.FreeBytes - before.FreeBytes,
//		AllocObjects: after.AllocObjects - before.AllocObjects,
//		FreeObjects:  after.FreeObjects - before.FreeObjects,
//	}
//}

type MemStats interface {
	Dump(name string)
}

func NewMemStats() MemStats {
	return newMemstatsDumper2()
}

func newMemstatsDumper2() MemStats {

	runtime.GC()
	time.Sleep(time.Second)
	runtime.GC()
	time.Sleep(time.Second)

	return &memstatsDumper2{
		start: getHeapInternal(),
	}
}

type memstatsDumper2 struct {
	start runtime.MemProfileRecord
}

func (m *memstatsDumper2) Dump(name string) {
	runtime.GC()
	time.Sleep(time.Second)
	runtime.GC()
	time.Sleep(time.Second)


	memStatsBefore := m.start
	memStatsAfter := getHeapInternal()
	//delta := getHeapDelta(memStatsBefore, memStatsAfter)

	//allocObjectsBefore, allocBytesBefore := scaleHeapSample(memStatsBefore.AllocObjects, memStatsBefore.AllocBytes, int64(runtime.MemProfileRate))
	inUseObjectsBefore, inUseBytesBefore := scaleHeapSample(memStatsBefore.InUseObjects(), memStatsBefore.InUseBytes(), int64(runtime.MemProfileRate))

	//allocObjectsAfter, allocBytesAfter := scaleHeapSample(memStatsAfter.AllocObjects, memStatsAfter.AllocBytes, int64(runtime.MemProfileRate))
	inUseObjectsAfter, inUseBytesAfter := scaleHeapSample(memStatsAfter.InUseObjects(), memStatsAfter.InUseBytes(), int64(runtime.MemProfileRate))

	heapAlloc := int64(inUseBytesAfter - inUseBytesBefore)
	heapObjects := int64(inUseObjectsAfter - inUseObjectsBefore)

	buffer := &bytes.Buffer{}
	_, _ = fmt.Fprintf(buffer, "MEM STATS: %s\n", name)
	_, _ = fmt.Fprintf(buffer, "    InUseBytes: %s\n", limit.ByteCountToHuman(heapAlloc))
	_, _ = fmt.Fprintf(buffer, "    InUseObjects: %s\n", limit.ByteCountToHuman(heapObjects))
	_, _ = os.Stdout.Write(buffer.Bytes())
}


// Repeat executes f every interval seconds until stopc is closed or f returns an error.
// It executes f once right after being called.
func Repeat(interval time.Duration, stopc <-chan struct{}, f func() error) error {
	tick := time.NewTicker(interval)
	defer tick.Stop()

	for {
		if err := f(); err != nil {
			return err
		}
		select {
		case <-stopc:
			return nil
		case <-tick.C:
		}
	}
}

// Retry executes f every interval seconds until timeout or no error is returned from f.
func Retry(interval time.Duration, stopc <-chan struct{}, f func() error) error {
	return RetryWithLog(log.NewNopLogger(), interval, stopc, f)
}

// RetryWithLog executes f every interval seconds until timeout or no error is returned from f. It logs an error on each f error.
func RetryWithLog(logger log.Logger, interval time.Duration, stopc <-chan struct{}, f func() error) error {
	tick := time.NewTicker(interval)
	defer tick.Stop()

	var err error
	for {
		if err = f(); err == nil {
			return nil
		}
		level.Error(logger).Log("msg", "function failed. Retrying in next tick", "err", err)
		select {
		case <-stopc:
			return err
		case <-tick.C:
		}
	}
}

// CloseWithLogOnErr is making sure we log every error, even those from best effort tiny closers.
func CloseWithLogOnErr(logger log.Logger, closer io.Closer, format string, a ...interface{}) {
	err := closer.Close()
	if err == nil {
		return
	}

	if logger == nil {
		logger = log.NewLogfmtLogger(os.Stderr)
	}

	level.Warn(logger).Log("msg", "detected close error", "err", errors.Wrap(err, fmt.Sprintf(format, a...)))
}

// ExhaustCloseWithLogOnErr closes the io.ReadCloser with a log message on error but exhausts the reader before.
func ExhaustCloseWithLogOnErr(logger log.Logger, r io.ReadCloser, format string, a ...interface{}) {
	_, err := io.Copy(ioutil.Discard, r)
	if err != nil {
		level.Warn(logger).Log("msg", "failed to exhaust reader, performance may be impeded", "err", err)
	}

	CloseWithLogOnErr(logger, r, format, a...)
}

// CloseWithErrCapture runs function and on error return error by argument including the given error (usually
// from caller function).
func CloseWithErrCapture(err *error, closer io.Closer, format string, a ...interface{}) {
	merr := tsdberrors.MultiError{}

	merr.Add(*err)
	merr.Add(errors.Wrapf(closer.Close(), format, a...))

	*err = merr.Err()
}

// ExhaustCloseWithErrCapture closes the io.ReadCloser with error capture but exhausts the reader before.
func ExhaustCloseWithErrCapture(err *error, r io.ReadCloser, format string, a ...interface{}) {
	_, copyErr := io.Copy(ioutil.Discard, r)

	CloseWithErrCapture(err, r, format, a...)

	// Prepend the io.Copy error.
	merr := tsdberrors.MultiError{}
	merr.Add(copyErr)
	merr.Add(*err)

	*err = merr.Err()
}

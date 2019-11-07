package limit

import (
	"fmt"
	"os"
	"strconv"
)

const (
	envQueryPipeLimit  = "QUERY_PIPE_LIMIT"
	envQueryTotalLimit = "QUERY_TOTAL_LIMIT"
)

var (
	// queryPipeLimit is the number of bytes querier can receive from one given source.
	// zero or negative == no limit
	queryPipeLimit int64

	// queryTotalLimit is the total number of bytes querier can receive from all sources overall.
	// zero or negative == no limit
	queryTotalLimit int64
)

func init() {
	queryPipeLimit = getLimitFromEnvVar(envQueryPipeLimit)
	queryTotalLimit = getLimitFromEnvVar(envQueryTotalLimit)
}

func getLimitFromEnvVar(envVarName string) int64 {
	var (
		parsedLimit int64
		err         error
	)

	if qpl := os.Getenv(envVarName); qpl != "" {
		parsedLimit, err = strconv.ParseInt(qpl, 10, 0)
		if err != nil {
			panic(fmt.Sprintf("Cannot parse %s as int: %v", envVarName, err))
		}
	}

	fmt.Printf("%s: %s\n", envVarName, LimitToHuman(parsedLimit))

	if parsedLimit <= 0 {
		return 0
	} else {
		return parsedLimit
	}
}

func LimitToHuman(n int64) string {
	if n <= 0 {
		return "OFF"
	}

	return ByteCountToHuman(n)
}

func ByteCountToHuman(n int64) string {
	const (
		kb = 1000
		mb = 1000 * kb
		gb = 1000 * mb
		tb = 1000 * gb
	)

	switch {
	case n >= tb:
		return fmt.Sprintf("%.2fTB", float64(n)/float64(tb))
	case n >= gb:
		return fmt.Sprintf("%.2fGB", float64(n)/float64(gb))
	case n >= mb:
		return fmt.Sprintf("%.2fMB", float64(n)/float64(mb))
	case n >= kb:
		return fmt.Sprintf("%.2fKB", float64(n)/float64(kb))
	default:
		return fmt.Sprintf("%d bytes", n)
	}
}

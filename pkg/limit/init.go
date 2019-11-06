package limit

import (
	"fmt"
	"os"
	"strconv"
)

const (
	envQueryPipeLimit = "QUERY_PIPE_LIMIT"
)

var (
	// queryPipeLimit is the total number of bytes querier can receive from one given source.
	// zero or negative == no limit
	queryPipeLimit int64
)

func init() {
	var (
		parsedQueryPipeLimit int64
		err                  error
	)

	if qpl := os.Getenv(envQueryPipeLimit); qpl != "" {
		parsedQueryPipeLimit, err = strconv.ParseInt(qpl, 10, 0)
		if err != nil {
			panic(fmt.Sprintf("Cannot parse %s as int: %v", envQueryPipeLimit, err))
		}
	}

	if parsedQueryPipeLimit <= 0 {
		queryPipeLimit = 0
	} else {
		queryPipeLimit = parsedQueryPipeLimit
	}

	fmt.Printf("QUERY_PIPE_LIMIT: %s\n", byteCountToHuman(queryPipeLimit))
	return
}

func byteCountToHuman(n int64) string {
	const (
		kb  = 1000
		mb  = 1000 * kb
		gb  = 1000 * mb
		tb  = 1000 * gb
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
	case n > 0:
		return fmt.Sprintf("%d bytes", n)
	default:
		return "OFF"
	}
}

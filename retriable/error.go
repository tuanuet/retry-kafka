package retriable

import (
	"errors"
	"fmt"
)

var (
	ErrorWithoutRetry = errors.New("error without retry")
)

// ErrorBatchHandler throw when has some msg error
// Indexes is empty mean: all msg is pass
type ErrorBatchHandler struct {
	Indexes []int
}

func (e ErrorBatchHandler) Error() string {
	return fmt.Sprintf("Error with retring idx: %v", e.Indexes)
}

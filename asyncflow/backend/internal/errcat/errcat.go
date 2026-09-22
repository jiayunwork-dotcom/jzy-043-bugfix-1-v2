package errcat

import "strings"

// Category buckets errors for dead-letter aggregation.
type Category string

const (
	CatTimeout      Category = "timeout"
	CatPanic        Category = "panic"
	CatPreempted    Category = "preempted"
	CatInvalidInput Category = "invalid_input"
	CatExternal     Category = "external_dependency"
	CatHandler      Category = "handler_error"
	CatUnknown      Category = "unknown"
)

// Classify maps a raw error string to a coarse category.
func Classify(errMsg string) Category {
	m := strings.ToLower(errMsg)
	switch {
	case strings.Contains(m, "timeout") || strings.Contains(m, "deadline exceeded") || strings.Contains(m, "context deadline"):
		return CatTimeout
	case strings.Contains(m, "panic"):
		return CatPanic
	case strings.Contains(m, "preempt") || strings.Contains(m, "interrupt"):
		return CatPreempted
	case strings.Contains(m, "invalid") || strings.Contains(m, "unmarshal") || strings.Contains(m, "bad request"):
		return CatInvalidInput
	case strings.Contains(m, "connection") || strings.Contains(m, "http") || strings.Contains(m, "dial") || strings.Contains(m, "refused"):
		return CatExternal
	case errMsg == "":
		return CatUnknown
	default:
		return CatHandler
	}
}

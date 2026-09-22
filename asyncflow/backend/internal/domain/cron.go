package domain

import (
	"fmt"
	"strings"
	"time"
)

// CronSchedule is a minimal 5-field cron expression parser
// (minute hour day-of-month month day-of-week), sufficient for retry timing.
type CronSchedule struct {
	minutes     []int
	hours       []int
	daysOfMonth []int
	months      []int
	daysOfWeek  []int
}

// ParseCron parses a standard 5-field cron expression.
func ParseCron(expr string) (*CronSchedule, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron expression must have 5 fields, got %d", len(fields))
	}
	s := &CronSchedule{}
	var err error
	parse := func(field string, min, max int) ([]int, error) {
		return expandCronField(field, min, max)
	}
	if s.minutes, err = parse(fields[0], 0, 59); err != nil {
		return nil, fmt.Errorf("minute: %w", err)
	}
	if s.hours, err = parse(fields[1], 0, 23); err != nil {
		return nil, fmt.Errorf("hour: %w", err)
	}
	if s.daysOfMonth, err = parse(fields[2], 1, 31); err != nil {
		return nil, fmt.Errorf("day-of-month: %w", err)
	}
	if s.months, err = parse(fields[3], 1, 12); err != nil {
		return nil, fmt.Errorf("month: %w", err)
	}
	if s.daysOfWeek, err = parse(fields[4], 0, 6); err != nil {
		return nil, fmt.Errorf("day-of-week: %w", err)
	}
	return s, nil
}

func expandCronField(field string, min, max int) ([]int, error) {
	var out []int
	seen := map[int]bool{}
	add := func(v int) {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	for _, part := range strings.Split(field, ",") {
		step := 1
		base := part
		if i := strings.Index(part, "/"); i >= 0 {
			stepPart := part[i+1:]
			var err error
			if step, err = atoiSmall(stepPart); err != nil || step <= 0 {
				return nil, fmt.Errorf("bad step %q", stepPart)
			}
			base = part[:i]
		}
		lo, hi := min, max
		switch {
		case base == "*":
			lo, hi = min, max
		case strings.HasPrefix(base, "*/"):
			lo, hi = min, max
		case strings.Contains(base, "-"):
			bounds := strings.SplitN(base, "-", 2)
			var err error
			if lo, err = atoiSmall(bounds[0]); err != nil {
				return nil, fmt.Errorf("bad range %q", base)
			}
			if hi, err = atoiSmall(bounds[1]); err != nil {
				return nil, fmt.Errorf("bad range %q", base)
			}
		default:
			v, err := atoiSmall(base)
			if err != nil {
				return nil, fmt.Errorf("bad value %q", base)
			}
			if step != 1 { // e.g. 5/10
				lo, hi = v, max
			} else {
				if v < min || v > max {
					return nil, fmt.Errorf("value %d out of range", v)
				}
				add(v)
				continue
			}
		}
		if lo < min || hi > max || lo > hi {
			return nil, fmt.Errorf("range %d-%d out of bounds %d-%d", lo, hi, min, max)
		}
		for v := lo; v <= hi; v += step {
			add(v)
		}
	}
	return out, nil
}

func atoiSmall(s string) (int, error) {
	if s == "" {
		return 0, fmt.Errorf("empty")
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("not a number: %s", s)
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

func contains(list []int, v int) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// Next returns the earliest instant after t matching the schedule.
func (s *CronSchedule) Next(t time.Time) time.Time {
	loc := t.Location()
	if loc == time.UTC {
		loc = time.UTC
	}
	// Start one minute later, second 0.
	cand := t.Add(time.Minute)
	cand = time.Date(cand.Year(), cand.Month(), cand.Day(),
		cand.Hour(), cand.Minute(), 0, 0, loc)

	limit := t.Add(366 * 24 * time.Hour)
	for cand.Before(limit) {
		if !contains(s.months, int(cand.Month())) {
			cand = time.Date(cand.Year(), cand.Month()+1, 1, 0, 0, 0, 0, loc)
			continue
		}
		domMatch := contains(s.daysOfMonth, cand.Day())
		dowMatch := contains(s.daysOfWeek, int(cand.Weekday()))
		if !domMatch || !dowMatch {
			cand = time.Date(cand.Year(), cand.Month(), cand.Day()+1, 0, 0, 0, 0, loc)
			continue
		}
		if !contains(s.hours, cand.Hour()) {
			cand = time.Date(cand.Year(), cand.Month(), cand.Day(),
				cand.Hour()+1, 0, 0, 0, loc)
			continue
		}
		if !contains(s.minutes, cand.Minute()) {
			cand = cand.Add(time.Minute)
			continue
		}
		return cand
	}
	return t.Add(24 * time.Hour)
}

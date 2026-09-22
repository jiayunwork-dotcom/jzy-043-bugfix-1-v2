package domain

import (
	"testing"
	"time"
)

func TestCronNext(t *testing.T) {
	loc := time.UTC
	// Every 5 minutes.
	s, err := ParseCron("*/5 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 9, 20, 10, 2, 0, 0, loc)
	next := s.Next(from)
	if want := time.Date(2026, 9, 20, 10, 5, 0, 0, loc); !next.Equal(want) {
		t.Fatalf("got %v want %v", next, want)
	}

	// Daily at 03:30.
	s2, err := ParseCron("30 3 * * *")
	if err != nil {
		t.Fatal(err)
	}
	from2 := time.Date(2026, 9, 20, 10, 0, 0, 0, loc)
	next2 := s2.Next(from2)
	want2 := time.Date(2026, 9, 21, 3, 30, 0, 0, loc)
	if !next2.Equal(want2) {
		t.Fatalf("got %v want %v", next2, want2)
	}
}

func TestCronValidation(t *testing.T) {
	if _, err := ParseCron("* * * *"); err == nil {
		t.Fatal("expected error for 4 fields")
	}
	if _, err := ParseCron("99 * * * *"); err == nil {
		t.Fatal("expected out-of-range error")
	}
	if _, err := ParseCron("0 0 * * 7"); err == nil {
		t.Fatal("expected out-of-range weekday error")
	}
}

func TestPriorityOrder(t *testing.T) {
	if PriorityCritical.Rank() != 0 || PriorityBulk.Rank() != 4 {
		t.Fatal("priority ranks wrong")
	}
	if !PriorityHigh.Valid() {
		t.Fatal("high should be valid")
	}
	if Priority("bogus").Valid() {
		t.Fatal("bogus priority must be invalid")
	}
}

package sim

import (
	"testing"
)

func TestRunsInTimeOrder(t *testing.T) {
	s := New(1)
	var order []uint64
	s.Schedule(30, func() { order = append(order, 30) })
	s.Schedule(10, func() { order = append(order, 10) })
	s.Schedule(20, func() { order = append(order, 20) })

	s.Run(100)

	want := []uint64{10, 20, 30}
	for i, v := range want {
		if order[i] != v {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
	if s.Now() != 30 {
		t.Errorf("Now() = %d, want 30", s.Now())
	}
}

func TestEqualTimeRunsInInsertionOrder(t *testing.T) {
	s := New(1)
	var order []int
	for i := range make([]struct{}, 5) {
		i := i
		s.Schedule(5, func() { order = append(order, i) })
	}
	s.Run(100)
	for i := range order {
		if order[i] != i {
			t.Fatalf("equal-time order = %v, want sorted", order)
		}
	}
}

func TestRunRespectsMaxSteps(t *testing.T) {
	s := New(1)
	count := 0
	for range make([]struct{}, 10) {
		s.Schedule(1, func() { count++ })
	}
	ran := s.Run(4)
	if ran != 4 || count != 4 {
		t.Fatalf("ran=%d count=%d, want 4/4", ran, count)
	}
}

func TestRandIsDeterministic(t *testing.T) {
	a, b := New(99), New(99)
	for range make([]struct{}, 20) {
		if a.Rand().Int63() != b.Rand().Int63() {
			t.Fatal("same seed produced different RNG sequence")
		}
	}
}

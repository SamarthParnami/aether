package sim

import (
	"fmt"
	"reflect"
	"testing"
)

func TestDeliversWithoutFaults(t *testing.T) {
	s := New(1)
	net := NewNetwork(s, FaultConfig{}) // perfect network
	got := []any{}
	net.Register("B", func(_ NodeID, msg any) { got = append(got, msg) })

	net.Send("A", "B", "hello")
	s.Run(100)

	if len(got) != 1 || got[0] != "hello" {
		t.Fatalf("got %v, want [hello]", got)
	}
}

func TestPartitionBlocksDelivery(t *testing.T) {
	s := New(1)
	net := NewNetwork(s, FaultConfig{})
	delivered := false
	net.Register("B", func(_ NodeID, _ any) { delivered = true })

	net.Partition("A", "B")
	net.Send("A", "B", 1)
	s.Run(100)
	if delivered {
		t.Fatal("message delivered across a partition")
	}

	net.Heal("A", "B")
	net.Send("A", "B", 2)
	s.Run(100)
	if !delivered {
		t.Fatal("message not delivered after heal")
	}
}

func TestDropAllDeliveries(t *testing.T) {
	s := New(1)
	net := NewNetwork(s, FaultConfig{DropProb: 1.0})
	count := 0
	net.Register("B", func(_ NodeID, _ any) { count++ })

	for range make([]struct{}, 50) {
		net.Send("A", "B", 1)
	}
	s.Run(1000)
	if count != 0 {
		t.Fatalf("DropProb 1.0 delivered %d messages, want 0", count)
	}
}

// The headline property: a faulty network replays bit-for-bit from its seed.
func TestDeterministicReplay(t *testing.T) {
	run := func(seed int64) []string {
		s := New(seed)
		net := NewNetwork(s, FaultConfig{DropProb: 0.3, DupProb: 0.2, MinDelay: 1, MaxDelay: 6})
		var log []string
		net.Register("B", func(from NodeID, msg any) {
			log = append(log, fmt.Sprintf("t%d:%s:%v", s.Now(), from, msg))
		})
		for i := range make([]struct{}, 20) {
			net.Send("A", "B", i)
		}
		s.Run(10000)
		return log
	}

	if a, b := run(7), run(7); !reflect.DeepEqual(a, b) {
		t.Fatalf("same seed produced different runs:\n a=%v\n b=%v", a, b)
	}
}

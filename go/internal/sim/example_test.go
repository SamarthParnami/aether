package sim_test

import (
	"fmt"
	"time"

	"github.com/SamarthParnami/aether/go/internal/sim"
)

// Example shows the basic pattern: register nodes on a Network, let them exchange
// messages, and drive delivery with the Sim. This is how the chaos tests will model the
// gateway/owner talking over a faulty link — here a simple request/response.
func Example() {
	s := sim.New(1)
	// A network with a fixed 1ms delivery delay (no drops/dups).
	net := sim.NewNetwork(s, sim.FaultConfig{MinDelay: time.Millisecond, MaxDelay: time.Millisecond})
	start := s.Now()

	// "server" echoes back to whoever pinged it.
	net.Register("server", func(from sim.NodeID, msg any) {
		net.Send("server", from, "pong:"+msg.(string))
	})
	// "client" prints replies it receives, with the virtual time elapsed.
	net.Register("client", func(_ sim.NodeID, msg any) {
		fmt.Printf("client received %q after %v\n", msg, s.Now().Sub(start))
	})

	net.Send("client", "server", "ping")
	s.Run(100) // drive the simulation until the queue drains

	// Output:
	// client received "pong:ping" after 2ms
}

// Example_partition shows fault injection: a partitioned link drops messages, and Heal
// restores delivery. Faults (drop/delay/reorder/duplicate/partition) are how the chaos
// tests model an owner losing contact with the rest of the cluster.
func Example_partition() {
	s := sim.New(1)
	net := sim.NewNetwork(s, sim.FaultConfig{}) // perfect network, immediate delivery

	net.Register("b", func(_ sim.NodeID, msg any) { fmt.Println("b received:", msg) })

	net.Partition("a", "b")
	net.Send("a", "b", "while-partitioned") // dropped
	s.Run(10)

	net.Heal("a", "b")
	net.Send("a", "b", "after-heal") // delivered
	s.Run(10)

	// Output:
	// b received: after-heal
}

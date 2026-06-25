package sim

// NodeID identifies a node in the simulated network.
type NodeID string

// Handler receives a message delivered to a node.
type Handler func(from NodeID, msg any)

// FaultConfig controls the faults the network injects. The zero value is a perfect
// network: no drops, no duplicates, immediate in-order delivery.
type FaultConfig struct {
	DropProb float64 // probability in [0,1) that a message is silently dropped
	DupProb  float64 // probability in [0,1) that a message is delivered twice
	MinDelay uint64  // minimum delivery delay in ticks
	MaxDelay uint64  // maximum delivery delay in ticks; > MinDelay makes delivery reorder
}

// Network is an in-memory message bus over a Sim. Delivery delay, drops, duplicates,
// reordering, and partitions are all driven by the Sim's seeded RNG, so the whole
// network is deterministic and replayable.
type Network struct {
	sim     *Sim
	cfg     FaultConfig
	nodes   map[NodeID]Handler
	blocked map[link]bool
}

type link struct{ a, b NodeID }

// NewNetwork creates a bus over sim with the given fault profile.
func NewNetwork(sim *Sim, cfg FaultConfig) *Network {
	return &Network{
		sim:     sim,
		cfg:     cfg,
		nodes:   map[NodeID]Handler{},
		blocked: map[link]bool{},
	}
}

// Register attaches a handler for a node id.
func (n *Network) Register(id NodeID, h Handler) { n.nodes[id] = h }

// Partition blocks the link between a and b in both directions until Heal.
func (n *Network) Partition(a, b NodeID) { n.blocked[canon(a, b)] = true }

// Heal restores a partitioned link.
func (n *Network) Heal(a, b NodeID) { delete(n.blocked, canon(a, b)) }

func (n *Network) isBlocked(a, b NodeID) bool { return n.blocked[canon(a, b)] }

// Send delivers msg from -> to, subject to the configured faults. Delivery is always
// scheduled on the Sim (never synchronous), so it interleaves with other nodes' work.
// A message is dropped if the link is partitioned, or by random drop; it may be
// duplicated; and it arrives after a (possibly randomized) delay. Reordering emerges
// when two sends draw different delays.
func (n *Network) Send(from, to NodeID, msg any) {
	if n.isBlocked(from, to) {
		return
	}
	if n.cfg.DropProb > 0 && n.sim.rng.Float64() < n.cfg.DropProb {
		return
	}
	n.scheduleDelivery(from, to, msg)
	if n.cfg.DupProb > 0 && n.sim.rng.Float64() < n.cfg.DupProb {
		n.scheduleDelivery(from, to, msg)
	}
}

func (n *Network) scheduleDelivery(from, to NodeID, msg any) {
	delay := n.cfg.MinDelay
	if n.cfg.MaxDelay > n.cfg.MinDelay {
		span := int64(n.cfg.MaxDelay - n.cfg.MinDelay + 1)
		delay += uint64(n.sim.rng.Int63n(span))
	}
	n.sim.Schedule(delay, func() {
		// The link may have partitioned between send and delivery.
		if n.isBlocked(from, to) {
			return
		}
		if h, ok := n.nodes[to]; ok {
			h(from, msg)
		}
	})
}

// canon orders a pair so a<->b and b<->a map to the same link.
func canon(a, b NodeID) link {
	if a > b {
		a, b = b, a
	}
	return link{a, b}
}

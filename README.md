# Aether

> *The rare, fluid element once thought to fill all space and connect everything.*

Aether is the greenfield real-time backbone for Inclass — a high-availability, high-SLA online
meeting platform with live **state sharing** across participants, redundant meeting backends
(LiveKit / Dyte), and seamless recovery **without reloading the tab**.

This folder holds the design and planning docs for the build.

## Documents

| Doc | Audience | What it is |
|-----|----------|------------|
| [01-design-backbone.md](01-design-backbone.md) | Engineering (deep) | The actionable design: why, the 3-phase plan, **Phase 1 in depth**, locked decisions, the quantitative "what happens if" FAQ, and the test strategy. |
| [02-brief-engineering.md](02-brief-engineering.md) | Engineering (skim) | One-page eng brief: the shape of the system, the headline decisions, what Phase 1 ships. |
| [03-brief-leadership.md](03-brief-leadership.md) | PM / Leadership | Plain-language: the problem, why we're rebuilding, the phased plan, risk & SLA posture. |

## Status

Design / pre-build. Architecture has converged; Phase 1 not yet started.

## The one-line architecture

> One disposable WebSocket per client. Correctness via **recovery** (sequence cursor + idempotent
> replay + ack-after-persist), not via redundant connections. A **per-room quorum log** is the only
> source of truth; everything else (socket nodes, Redis) is disposable and rebuildable.
> Redundancy lives in **many interchangeable nodes across AZs + fast re-homing**, not in the client.

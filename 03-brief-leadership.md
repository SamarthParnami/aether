# Aether — Leadership Brief

> *Aether: the element once thought to fill all space and connect everything.*
> A new, highly-reliable foundation for Inclass live classrooms.

## What it is, in one line
Aether is a ground-up rebuild of the real-time engine behind Inclass — the part that keeps every
participant's screen in sync (current slide, raised hands, who's presenting) and keeps the class running
smoothly even when something fails behind the scenes.

## Why we're doing it
Today's Inclass real-time layer works, and it's more capable than "a simple relay" — it already saves
classroom state and can run on multiple servers. But (after reviewing the actual code) it's missing the
*foundations* needed for true reliability, and those can't be patched in — they're architectural:
- **Partial recovery.** If a student's connection blips, some things come back (mic, screen-share) but the
  **lesson content does not reliably re-sync** — there's no ordered record of "what happened, in what
  order," so a reconnecting student can quietly fall out of sync on slides/content.
- **No clean failover.** It can run on several servers, but no server "owns" a given class, so if one
  restarts or fails, students are bounced to another server with no coordinated handoff — disruption can be
  visible to the class.
- **No first-class admin control.** There's no privileged admin role that can reliably override or take
  control; today it's tutor-only, with no override path.

For a platform where a frozen or out-of-sync classroom directly hurts the learning experience, these gaps
are a ceiling on quality and reliability we can't raise by patching the current foundation. Aether rebuilds
that foundation — an ordered, authoritative record of every class, automatic recovery, coordinated server
failover, and admin override built in.

## What changes for users
- **Classes don't break on a hiccup.** A dropped connection, a server restart, or even a data-center zone
  failure recovers automatically — **without reloading the tab**. At worst, a brief pause, then it picks up
  exactly where it was.
- **Everyone sees the same thing, reliably**, with the teacher/admin always able to take control.
- **Built for India.** Hosted in AWS Mumbai — fast across the country (sub-100ms), and keeping data in
  India supports our obligations under India's data-protection law (DPDP) for students' data. *(Region
  choice is necessary but not sufficient for compliance — consent, retention and minors'-data handling are
  separate workstreams; this is not a legal sign-off.)*

## How we're building it — three phases
| Phase | What it delivers | Visible to users? |
|-------|------------------|-------------------|
| **1 — Foundation** | The reliability engine: redundant servers, automatic recovery, and a fault-injection test suite that *deliberately breaks things* to prove it heals. | No — internal plumbing. |
| **2 — Basic experience** | A simple working classroom on the new engine: shared state, teacher/admin override, saved reliably. | Demo / internal. |
| **3 — Inclass on Aether** | Real Inclass features moved onto the proven engine, including redundant video providers (LiveKit + Dyte). | Yes — the new Inclass. |

We deliberately build the **reliability first** and prove it before adding features — so we're never
debugging "is it the feature or the foundation?" in production.

## Reliability posture
> The numbers below are **engineering estimates / targets**, derived from AWS's published service levels
> and our assumed recovery times — **not yet measured**, because the system isn't built. They're for
> setting expectations and sizing effort, and will be replaced with real data once we have it. (Derivation
> in the engineering design doc, §12.)
- **Target:** survives the failures that actually happen — server crashes, deploys, and an entire AWS
  availability-zone going down — automatically and (for users) near-invisibly. Our derivation puts this in
  the **~99.9–99.95% band in a normal year**, with the ceiling set by AWS's database service (~99.99%) and
  dragged down by our own failover windows and ordinary operational reality.
- **One deliberate, accepted risk:** if AWS Mumbai has a rare **whole-region** outage (notable AWS
  regional incidents happen a few times a year *across all regions globally*; for any single region like
  Mumbai it's rarer still — but when one hits it can last minutes to a few hours), Inclass would be
  degraded for that window. We're choosing **not** to build the very expensive multi-region/multi-cloud insurance now,
  because it would add a rarely-tested failure path that tends to *cause* outages, for a risk this size.
  **We keep the door open** to add an in-India backup (AWS Hyderabad) later if a contract or SLA ever
  requires it — the design is built to allow it without a rewrite.
- **Every other failure mode** has a defined, tested behavior: worst case is a brief, automatic pause —
  never lost data, never a corrupted classroom, never a forced reload. (Full failure-by-failure analysis
  with probabilities is in the engineering design doc.)

## What we're explicitly NOT doing (and why it's the right call)
- **No multi-cloud / no cross-region active-active.** Insures the rarest failure at the highest ongoing
  cost, and historically lowers real-world reliability. Not worth it for our risk profile.
- **No second compute system as backup.** It wouldn't cover the risks that matter and doubles operational
  burden. We invest instead in doing one setup excellently.

## What we need
- Engineering focus for **Phase 1** (foundation + test harness) before feature work.

## Bottom line
Aether trades a one-time foundational investment for a classroom experience that **stays up and stays in
sync through real-world failures**, built right for India, with a clear, honest line on the one rare risk
we're choosing to accept — and a clean path to close even that later if the business needs it.

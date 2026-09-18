# Chronos-µVM
### A Deterministic, Fault-Tolerant Micro-Virtual Machine for Secure Multi-Tenant Serverless Workloads

**Status:** Core architecture + reference implementation (Rust)
**Scope:** Bare-metal supervisor, deterministic execution clock, Wasm sandbox isolation, io_uring I/O plane, SMT-based execution-integrity checkpointing, live state-diff HA replication.

---

## 1. System Overview

Chronos-µVM is a userspace supervisor that hosts many isolated Wasm-bytecode tenants ("cells") inside a single host process, without a general-purpose container runtime or a full hardware hypervisor (KVM is *optional*, used only as a nested-paging fast path — see §6). The design goal is **replayable determinism**: given the same recorded input trace, any two physical hosts produce bit-identical heap and register state at every checkpoint boundary. This is what allows the live-replication protocol (§7) to fail over a standby node without re-executing from genesis, and what allows the SMT checkpoint chain (§6) to serve as a cryptographic execution-integrity proof, not just a fault-tolerance mechanism.

```
┌──────────────────────────────────────────────────────────────────────┐
│                          CHRONOS-µVM HOST PROCESS                     │
│                                                                        │
│  ┌───────────────┐   ┌───────────────┐   ┌───────────────┐            │
│  │   Cell #0     │   │   Cell #1     │   │   Cell #N     │  ...       │
│  │ (Wasm tenant) │   │ (Wasm tenant) │   │ (Wasm tenant) │            │
│  └──────┬────────┘   └──────┬────────┘   └──────┬────────┘            │
│         │  syscall/host-call interception (§3)   │                    │
│  ┌──────▼───────────────────▼───────────────────▼────────┐            │
│  │             DETERMINISTIC SCHEDULER (§2)               │            │
│  │  - logical clock (LamportTick)                          │           │
│  │  - epoch-bounded quantum dispatch                        │          │
│  │  - non-determinism capture (RNG, wall-clock, IRQ)        │          │
│  └──────┬──────────────────────────────────┬──────────────┘            │
│         │                                   │                          │
│  ┌──────▼─────────────┐            ┌────────▼──────────────┐           │
│  │ MEMORY / SANDBOX    │            │  CHECKPOINT PIPELINE   │           │
│  │ - linear mem guards │            │  - SMT hash over heap  │           │
│  │ - UFFD lazy paging  │            │  - register-file digest│           │
│  │ - delta-diff engine │            │  - Merkle chain root    │          │
│  └──────┬─────────────┘            └────────┬──────────────┘           │
│         │                                    │                          │
│  ┌──────▼─────────────┐            ┌─────────▼─────────────┐           │
│  │ io_uring I/O RING   │            │ REPLICATION STREAM     │           │
│  │ - SQE/CQE zero-copy │            │ - lock-free ring buf   │           │
│  │ - per-cell fd table │            │ - delta stream to      │           │
│  │                     │            │   hot standby           │          │
│  └─────────────────────┘            └────────────────────────┘           │
└──────────────────────────────────────────────────────────────────────┘
```

Crate layout (Cargo workspace):

| Crate | Responsibility |
|---|---|
| `chronos-core` | Supervisor loop, cell lifecycle, deterministic scheduler, syscall interception |
| `chronos-mem` | Linear-memory guards, page-fault (UFFD) handling, delta-diff engine, SMT checkpoint tree |
| `chronos-io` | io_uring submission/completion ring abstraction, zero-copy guest↔host I/O mapping |
| `chronos-replicate` | Lock-free SPSC/MPSC delta stream, wire protocol, standby apply loop |

---

## 2. Deterministic Execution Clock

Determinism is broken by three classes of input: **wall-clock reads**, **PRNG draws**, and **asynchronous interrupts / I/O completion ordering**. Chronos-µVM does not try to prevent tenants from asking for these — it intercepts every such request at the host-call boundary and turns it into a *recorded, replayable event*.

**Logical clock.** Every cell executes under a `LogicalClock`, a monotonically increasing 64-bit tick counter advanced only by the scheduler, never by wall time. One tick = one dispatched execution quantum (a bounded number of Wasm instructions, configurable, default 100k). The scheduler is a strict round-robin (extendable to weighted-fair) dispatcher: it never re-orders cells based on I/O readiness, because I/O readiness itself is a non-deterministic input that must be captured, not observed.

**Event log.** Any host-call that would touch real non-determinism (`clock_time_get`, `random_get`, io_uring CQE delivery) is redirected into an `EventRecorder`:
- *Record mode*: the real value is fetched once, appended to an append-only per-cell log keyed by `(tick, call_ordinal)`, and returned to the tenant.
- *Replay mode*: the same `(tick, call_ordinal)` key is looked up in a previously captured log and returned verbatim; the real syscall is never issued.

Because the key is `(tick, call_ordinal)` rather than wall-clock time, replay is host-independent: a standby node with a different CPU frequency and different kernel entropy pool reproduces the exact same tenant-visible values.

**Quantum boundary discipline.** Interrupts (including io_uring CQEs) are only allowed to become visible to a cell at a quantum boundary. Between boundaries the cell runs uninterrupted; this removes the entire class of "which instruction was executing when the interrupt landed" nondeterminism.

---

## 3. Syscall / Host-Call Interception

Tenants never see a raw OS syscall. All host interaction goes through a fixed **host-call ABI** (Wasm imports resolved to Rust trampolines):

- `fd_read`, `fd_write`, `fd_close` → routed through `chronos-io`'s per-cell file-descriptor table, which maps a cell-local fd to a real fd registered with the io_uring instance.
- `clock_time_get`, `random_get` → routed through the `EventRecorder` (§2).
- `mem_grow` → routed through `chronos-mem`'s guarded linear-memory allocator, which enforces the tenant's memory quota and re-arms UFFD watch regions.

No tenant ever holds a raw pointer into host memory; every guest pointer is validated against the cell's linear-memory bounds before dereference (see `GuardedMemory::translate` in the code, §9).

---

## 4. Zero-Overhead Sandbox Isolation

Two isolation layers, chosen per deployment profile:

1. **Software fault isolation (default, no hardware requirement).** Each cell's linear memory is a single contiguous `mmap` region sized to the next power-of-two above the tenant's quota, followed by an unmapped (`PROT_NONE`) guard region at least one page wide. All Wasm loads/stores are bounds-checked against the *declared* memory size before translation (belt) and the guard page catches any bug in that check (suspenders) by delivering `SIGSEGV`, which the supervisor's signal handler converts into a clean cell-fault rather than a host crash.
2. **Hardware-assisted (optional fast path).** Where KVM is available, each cell may instead run inside a minimal nested-paging guest with a single-purpose page-fault vmexit handler; this removes the software bounds-check from the hot path at the cost of vmexit latency on first-touch faults. Selection between (1) and (2) is a per-tenant policy, not an architectural fork — both feed the same `GuardedMemory` trait (§9) so the delta-diff and SMT checkpoint code is isolation-mode-agnostic.

Cross-tenant leakage is structurally prevented because no cell's guest-pointer arithmetic can ever produce an address outside its own `mmap` region — the guard page is unmapped, not merely permission-denied-and-ignored.

---

## 5. io_uring-Powered Asynchronous I/O

One `io_uring` instance is shared across all cells on a given worker thread (SQPOLL mode optional). Submission is zero-copy: guest buffers are registered once via `IORING_REGISTER_BUFFERS` / fixed buffers, so a guest `fd_write` becomes a single `io_uring_prep_write_fixed` with no intermediate host-side copy.

Completion delivery is deferred to the next quantum boundary of the owning cell (§2) — CQEs are drained into a per-cell pending-completion queue by the supervisor's polling thread, and only handed to the tenant, in log order, when that cell is next dispatched. This is what makes I/O completion order part of the recorded event log rather than a live race.

---

## 6. Cryptographic SMT State Checkpointing

At a configurable tick interval (default: every 4,096 ticks), the supervisor computes a checkpoint:

1. The cell's linear memory is walked page-by-page (4 KiB pages). Each page's content is hashed (BLAKE3) into a leaf.
2. Leaves are placed into a **Sparse Merkle Tree** keyed by page index — sparse because most tenant heaps are mostly zero/unmapped, and an SMT lets those subtrees collapse to a well-known default-hash without being materialized.
3. The register file / execution-context snapshot (program counter equivalent, Wasm operand-stack summary, tick number) is hashed and included as an additional leaf at a reserved index.
4. The resulting SMT root, plus the previous checkpoint's root, are hashed together into a **checkpoint chain entry** — this is what makes the checkpoints a chain of execution-integrity proofs: reproducing root `R_n` requires having executed ticks `[0, n]` deterministically, and any auditor holding the recorded event log (§2) can independently recompute `R_n` and compare.

Only *dirty* pages (tracked by the delta-diff engine, §7) are re-hashed on each checkpoint; clean subtrees reuse their previous root, making checkpoint cost proportional to pages touched since the last checkpoint, not total heap size.

---

## 7. Live State-Diff Replication (High Availability)

Between checkpoints, the same dirty-page tracking that feeds the SMT is streamed to a hot standby:

1. Dirty pages are captured as they're marked (either via the UFFD `WP` write-protect + fault-on-write path, or via the software-SFI bounds-checked store path recording its own page index — both funnel into the same `DirtyPageSet`).
2. Each dirty page is delta-compressed against its previous known-good copy (XOR + run-length of zero runs is sufficient for typical heap mutation locality; a byte-level diff is used only when XOR-density is poor).
3. Deltas are pushed into a **lock-free SPSC ring buffer** (`chronos-replicate::DeltaRing`, built on `crossbeam::queue::ArrayQueue` semantics with a custom cache-line-padded head/tail) — one producer (the cell's dirty-tracking hook), one consumer (the network-streaming task).
4. The consumer batches deltas per quantum boundary and streams them, in tick order, to the standby node.
5. The standby applies deltas in tick order and independently recomputes SMT roots; if its computed root ever diverges from the primary's declared root for the same tick, that's a detected corruption/bug, not just a failover signal.

Failover is "promote standby, resume dispatch from `last_applied_tick + 1`" — because the standby's heap state, register state, *and* the deterministic event log up to that tick are all present, resumed execution is bit-identical to what the primary would have produced.

---

## 8. Hardware-Assisted Page Fault Interception (UFFD / Nested Paging)

Two lazy-paging strategies, matching the isolation modes of §4:

- **`userfaultfd` (software mode).** The cell's memory region is registered with `UFFDIO_REGISTER` for missing-page and (optionally) write-protect faults. A dedicated fault-handling thread reads `uffd` events and resolves them: a missing-page fault is satisfied with `UFFDIO_COPY` from a zero-page or a checkpoint-restored page; a write-protect fault (used for dirty tracking, §7) is resolved with `UFFDIO_WRITEPROTECT` removal after recording the page as dirty. This gives page-granularity dirty tracking without instrumenting every store instruction.
- **Nested paging (hardware mode).** Where KVM is used, the guest's second-level page tables start with all pages absent; the vmexit handler on EPT violation performs the equivalent resolution (map from checkpoint or zero-fill) and, for dirty tracking, leverages the hardware **dirty-bit** in the EPT entry directly (scanned per quantum boundary) instead of software write-protection faults — strictly lower overhead per write, at the cost of requiring virtualization extensions.

Both strategies expose the same `PageFaultResolver` trait (§9) to the rest of the system, so `chronos-mem`'s delta-diff and SMT code is written once against the trait, not against either backend.

---

## 9. Core Rust Interfaces (summary — full code in accompanying files)

```rust
pub trait GuardedMemory {
    fn translate(&self, guest_ptr: u32, len: u32) -> Result<*mut u8, MemFault>;
    fn grow(&mut self, delta_pages: u32) -> Result<u32, MemFault>;
    fn dirty_pages(&self) -> &DirtyPageSet;
}

pub trait PageFaultResolver {
    fn resolve_missing(&mut self, page_idx: u64) -> Result<(), MemFault>;
    fn resolve_write_fault(&mut self, page_idx: u64) -> Result<(), MemFault>;
}

pub trait NonDeterminismSource {
    fn wall_clock_now(&mut self, cell: CellId, tick: Tick, ordinal: u64) -> u64;
    fn random_bytes(&mut self, cell: CellId, tick: Tick, ordinal: u64, buf: &mut [u8]);
}
```

Full implementations follow in `chronos-core`, `chronos-mem`, `chronos-io`, `chronos-replicate`.

---

## 10. Known Limitations / Honest Caveats

This blueprint and the accompanying code are a **reference architecture and skeleton implementation**, not a drop-in production hypervisor:
- The io_uring submission loop uses the `io-uring` crate's safe(r) wrapper API rather than raw syscalls, to keep `unsafe` scoped and auditable; a maximally-optimized version would hand-roll the SQE/CQE ring.
- KVM nested-paging integration is specified architecturally (§4, §8) but not implemented in code here — it requires a VMM crate (e.g. `kvm-ioctls`) and guest firmware, which is a separate, large subsystem.
- The SMT implementation here is a correct, working sparse Merkle tree over BLAKE3 leaves; it is not wired to a specific zk-proof system, though its root is exactly the value such a system would consume.
- Wasm bytecode execution itself is assumed to come from an existing interpreter/compiler (e.g. `wasmi` or `wasmtime`) invoked at each quantum boundary; Chronos-µVM is the supervisor *around* that engine, not a Wasm interpreter itself.

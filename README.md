# Chronos-µVM

A deterministic, fault-tolerant micro-VM supervisor for secure multi-tenant
serverless workloads, written in Rust.

- **`docs/ARCHITECTURE.md`** — full system architecture blueprint: data flow,
  execution lifecycle, io_uring integration, cryptographic checkpoint
  pipeline, live replication protocol, deterministic clock loop, and an
  honest list of what is specified-but-not-implemented.
- **`chronos-mem/`** — guarded linear memory (mmap + guard page SFI),
  dirty-page tracking, delta-compression engine, Sparse Merkle Tree
  execution-integrity checkpointing.
- **`chronos-io/`** — io_uring-backed zero-copy I/O subsystem, per-cell fd
  isolation, deferred-to-quantum-boundary completion delivery.
- **`chronos-replicate/`** — lock-free SPSC delta ring buffer and standby
  apply loop for live state-diff HA replication.
- **`chronos-core/`** — the supervisor: deterministic logical clock,
  non-determinism record/replay event log, cell lifecycle, round-robin
  scheduler, and the top-level dispatch loop wiring everything together.

## Building

This workspace targets a standard Rust toolchain (edition 2021) with network
access to crates.io for its dependencies (`blake3`, `crossbeam`, `io-uring`,
`libc`, `parking_lot`, `thiserror`). From the workspace root:

```
cargo build --workspace
cargo test --workspace
```

`chronos-io`'s real `io_uring` backend (`linux_ring.rs`) only compiles on
Linux; non-Linux builds fall back to `StubIoSubsystem`, which implements the
same `IoSubsystem` trait for development and testing.

## What's a reference implementation vs. production-ready

See **§10 of `docs/ARCHITECTURE.md`** for the explicit list — most notably,
KVM/nested-paging integration is specified but not implemented here, and the
per-tick `DeltaEngine` wiring in `Supervisor::run_one_cycle` demonstrates the
data flow rather than the production ownership model (one long-lived
`DeltaEngine` per cell, not reconstructed per tick).

## 👤 About the Builder

**NIKHIL CHARY SRIRAMOJU**
B.Tech Final Year — Computer Science & Engineering

- 🔗 LinkedIn: [nikhil-chary-sriramoju](https://in.linkedin.com/in/nikhil-chary-sriramoju-95041b38a)
- 💻 GitHub: [Nikhil-creat](https://github.com/Nikhil-creat)
- 📧 Email: sriramojunikhil66@gmail.com
- 📸 Instagram: [nikhil__sriramoju](https://www.instagram.com/nikhil__sriramoju)

  

---
title = "Concurrency Model"
description = "The current single-writer, multi-reader contract exposed by the native engine and facade APIs."
layout = "reference"
eyebrow = "Correctness Contract"
lead = "Nexum currently standardizes on a single-writer and multi-reader model so committed reads stay stable while writes advance through a private overlay."
statusLabel = "Isolation Surface"
accent = "Committed snapshots only"
---

# Concurrency Model

Status: active implementation contract

Nexum currently adopts a single-writer/multi-reader model.

## Guarantee

- Any number of read operations may be in flight concurrently at the API layer.
- Write operations are exclusive.
- A single writer may be active while readers are also active.
- Readers observe last committed state only.
- The active writer observes its own private overlay plus the last committed state.
- Nexum does not expose dirty reads or partially applied transaction state.

## Scope

This guarantee applies to:

- the native object facade
- the native relational facade
- the native graph facade
- the Micronaut graph service wrapper

The storage engine also exposes `TransactionMode.READ_ONLY` and enforces the same contract for direct engine callers.

## Current Implementation Notes

- The contract is about correctness and isolation, not peak parallel throughput.
- Readers use an atomically published committed snapshot while the writer works in private overlay state.
- Commit publishes new committed state only after record, schema, root, and index changes are ready.
- Only one write transaction may be active at a time.
- There is no MVCC yet.
- There is no deadlock detection because the current model does not permit multiple concurrent writers.

## Non-Goals For This Phase

- multi-writer transactions
- snapshot isolation
- distributed coordination
- lock escalation
- cross-process writer coordination

## Operational Guidance

- Use the embedded facades for normal access patterns.
- Treat the HTTP query layer as an experimental API surface over the graph facade, not as the primary concurrency boundary.
- If a workload needs high-throughput concurrent reads during sustained writes, benchmark it explicitly before assuming the current implementation is sufficient.

---
layout: home
title: Floecat
permalink: /
author_profile: false
classes: wide
---

Floecat is a metadata control plane for Iceberg and Delta.

It gives query engines one consistent, planner-ready view across fragmented catalogs.

## Why Floecat exists

Lakehouse metadata is fragmented across catalogs, clouds, and engines.

Open table formats solved storage interoperability, but not:

- planner-ready statistics
- consistent semantics across catalogs
- cross-engine compatibility

Floecat is built for **consumption**, not mutation. It gives query engines a stable, consistent metadata layer instead of forcing each engine to rebuild its own.

## What Floecat does

- **Federation** — Access metadata across Iceberg and Delta catalogs through one API.
- **Canonical model** — Normalize heterogeneous metadata into a consistent graph.
- **Planner statistics** — Serve NDV, histograms, MCVs, and other statistics for cost-based optimization.
- **Deterministic planning** — Pin snapshots across catalogs to guarantee consistent replanning.

## Get started

```bash
git clone https://github.com/eng-floe/floecat.git
make run
make cli-run

Point Floecat at an Iceberg or Delta catalog, then query through its APIs or connect an engine like DuckDB.

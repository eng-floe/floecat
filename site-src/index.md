---
layout: page
title: Floecat
permalink: /
---

<img src="{{ '/images/floecat.png' | relative_url }}" alt="Floecat logo" style="height: 200px;">

Floecat is a catalog-of-catalogs for modern data lakehouses. It federates metadata from Iceberg and Delta sources, normalizes it into a canonical model, and serves planner-ready metadata, statistics, and interoperability APIs for engines and applications.

## Why Floecat exists

Lakehouse metadata is fragmented. Teams often end up with multiple catalogs across clouds, engines, and business domains, while open table formats still do not provide the planner-facing statistics and consistent semantics needed for high-quality query optimization. Floecat exists to close that gap with a metadata control plane that is consumption-oriented, not just mutation-oriented.  

## What Floecat does

- Federates metadata access across Iceberg and Delta ecosystems
- Stores a canonical representation for consistent downstream access
- Serves planner-ready statistics for cost-based optimization
- Supports protocol and engine interoperability
- Enables deterministic planning through cross-catalog snapshot pinning

## Where it fits

Floecat sits between query engines and upstream catalogs. It gives systems like Floe, DuckDB, Trino, and custom applications a stable metadata layer over fragmented sources rather than forcing every engine to solve normalization, statistics, and policy translation on its own.

## Get started

Clone the repo, run the quick start, point Floecat at an Iceberg or Delta catalog, and start querying through its APIs.

- <a href="https://github.com/eng-floe/floecat">Repository</a>
- <a href="https://github.com/eng-floe/floecat/blob/main/README.md">README</a>
- <a href="/floecat/blog/">Blog</a>


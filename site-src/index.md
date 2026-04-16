---
layout: page
title: Floecat
permalink: /
---

<img src="{{ '/images/floecat.png' | relative_url }}" alt="Floecat logo" style="height: 64px;">

<div class="hero">
  <p class="eyebrow">Open metadata control plane for the lakehouse</p>
  <h1>One planning view across fragmented catalogs</h1>
  <p class="lead">
    Floecat is a catalog-of-catalogs for modern data lakehouses. It federates metadata from Iceberg and Delta sources, normalizes it into a canonical model, and serves planner-ready metadata, statistics, and interoperability APIs for engines and applications.
  </p>

  <div class="hero-actions">
    <a class="button button-primary" href="https://github.com/eng-floe/floecat">View on GitHub</a>
    <a class="button" href="/floecat/blog/">Read the blog</a>
  </div>
</div>

<div class="feature-grid">
  <div class="feature-card">
    <div class="feature-icon">🗂️</div>
    <h2>Catalog-of-catalogs</h2>
    <p>
      Federate metadata across upstream catalogs, metastores, and table formats instead of forcing one engine-centric metadata world.
    </p>
  </div>

  <div class="feature-card">
    <div class="feature-icon">🔄</div>
    <h2>Canonical metadata model</h2>
    <p>
      Normalize heterogeneous source metadata into a consistent resource graph across account, catalog, namespace, table or view, snapshot, and statistics.
    </p>
  </div>

  <div class="feature-card">
    <div class="feature-icon">📈</div>
    <h2>Planner-ready statistics</h2>
    <p>
      Go beyond mutation-oriented catalogs by serving the statistics query planners actually need: row counts, NDV, null counts, histograms, min/max, MCVs, and more.
    </p>
  </div>

  <div class="feature-card">
    <div class="feature-icon">📌</div>
    <h2>Deterministic planning</h2>
    <p>
      Pin snapshots across catalogs, then plan and re-plan against the same frozen metadata universe for repeatable optimization behavior.
    </p>
  </div>

  <div class="feature-card">
    <div class="feature-icon">🔌</div>
    <h2>Interoperability layer</h2>
    <p>
      Expose unified APIs for discovery, authorization, query planning, and protocol compatibility rather than treating metadata as a format-specific implementation detail.
    </p>
  </div>

  <div class="feature-card">
    <div class="feature-icon">🤖</div>
    <h2>Built for the agentic future</h2>
    <p>
      Give engines, agents, governance tools, and custom applications one consistent semantic layer over fragmented lakehouse metadata.
    </p>
  </div>
</div>

## Why Floecat exists

Lakehouse metadata is fragmented. Teams often end up with multiple catalogs across clouds, engines, and business domains, while open table formats still do not provide the planner-facing statistics and consistent semantics needed for high-quality query optimization. Floecat exists to close that gap with a metadata control plane that is consumption-oriented, not just mutation-oriented.  [oai_citation:2‡GitHub](https://github.com/eng-floe/floecat?utm_source=chatgpt.com)  [oai_citation:3‡Floe_ a SQL compute service for the data lakehouse.pptx](sediment://file_000000005cd471fba5572bbc100ef3e2)

## What Floecat does

- Federates metadata access across Iceberg and Delta ecosystems
- Stores a canonical representation for consistent downstream access
- Serves planner-ready statistics for cost-based optimization
- Supports protocol and engine interoperability
- Enables deterministic planning through cross-catalog snapshot pinning

## Where it fits

Floecat sits between query engines and upstream catalogs. It gives systems like Floe, DuckDB, Trino, and custom applications a stable metadata layer over fragmented sources rather than forcing every engine to solve normalization, statistics, and policy translation on its own.  [oai_citation:4‡Floe_ a SQL compute service for the data lakehouse.pptx](sediment://file_000000005cd471fba5572bbc100ef3e2)

## Get started

Clone the repo, run the quick start, point Floecat at an Iceberg or Delta catalog, and start querying through its APIs.

- <a href="https://github.com/eng-floe/floecat">Repository</a>
- <a href="https://github.com/eng-floe/floecat/blob/main/README.md">README</a>
- <a href="/floecat/blog/">Blog</a>


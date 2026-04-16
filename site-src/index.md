---
layout: page
title: Floecat
permalink: /
---

<div class="hero">
  <div class="hero-header">
    <img src="{{ '/images/floecat.png' | relative_url }}" alt="Floecat logo">
    <div>
      <h1>Floecat</h1>
      <p class="lead">
        A metadata control plane for Iceberg and Delta that gives query engines one consistent, planner-ready view across fragmented catalogs.
      </p>
      <p class="subtle">
        Normalize metadata. Serve real statistics. Plan deterministically.
      </p>
    </div>
  </div>
</div>

---

## Why Floecat exists

Lakehouse metadata is fragmented across catalogs, clouds, and engines.

Open table formats solved storage interoperability, but not:
- planner-ready statistics  
- consistent semantics across catalogs  
- cross-engine compatibility  

Floecat is built for **consumption**, not mutation. It gives query engines a stable, consistent metadata layer instead of forcing each engine to rebuild its own.

---

## What Floecat does

<div class="feature-grid">

<div class="feature-card">
<h3>Federation</h3>
<p>Access metadata across Iceberg and Delta catalogs through one API.</p>
</div>

<div class="feature-card">
<h3>Canonical model</h3>
<p>Normalize heterogeneous metadata into a consistent graph.</p>
</div>

<div class="feature-card">
<h3>Planner statistics</h3>
<p>Serve NDV, histograms, MCVs, and other statistics for cost-based optimization.</p>
</div>

<div class="feature-card">
<h3>Deterministic planning</h3>
<p>Pin snapshots across catalogs to guarantee consistent replanning.</p>
</div>

</div>

---

## Where it fits

Floecat sits between query engines and upstream catalogs:

Engines / Apps → Floecat → Iceberg / Delta catalogs

Instead of every engine solving:
- metadata normalization  
- statistics collection  
- policy translation  

Floecat provides a shared control plane for all of them.

---

## Get started

```bash
git clone https://github.com/eng-floe/floecat.git
make run
make cli-run
```

Point Floecat at an Iceberg or Delta catalog, then query through its APIs or connect an engine like DuckDB.

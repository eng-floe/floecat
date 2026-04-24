---
layout: home
title: Floecat
permalink: /
author_profile: false
classes: wide
---

<section class="home-hero">
  <div class="home-hero__content">
    <p class="home-kicker">Metadata Control Plane</p>
    <h1>Planner-ready metadata for Iceberg and Delta</h1>
    <p class="home-subtitle">
      Built for data platform engineers and query engine teams that need consistent
      metadata semantics across catalogs, clouds, and engines.
    </p>
    <p class="home-attribution">
      Developed by <a href="https://floedb.ai/">FloeDB</a>.
    </p>
    <div class="home-actions">
      <a class="btn btn--primary home-btn" href="{{ '/documentation/docker/' | relative_url }}">Run Local Stack</a>
      <a class="btn btn--inverse home-btn" href="{{ '/documentation/' | relative_url }}">Read Docs</a>
    </div>
  </div>
  <div class="home-hero__brand">
    <img src="{{ '/images/floecat-hero.jpg' | relative_url }}" alt="Floecat logo" width="439" height="440" fetchpriority="high" loading="eager" decoding="async">
  </div>
</section>

{% assign latest = site.posts | first %}

{% if latest %}

<section id="latest-post" class="home-latest-post">
  <p class="home-latest-post__label">Latest from Floecat</p>
  <h2>{{ latest.title }}</h2>
  <p class="home-latest-post__meta">
    {{ latest.date | date: "%Y-%m-%d" }}
    {% assign latest_words = latest.content | strip_html | number_of_words %}
    {% assign latest_read = latest_words | divided_by: 180 | plus: 1 %}
    · {{ latest_read }} min read
  </p>
  {% assign latest_summary = latest.summary | default: latest.description | default: latest.excerpt %}
  <p>{{ latest_summary | strip_html | normalize_whitespace | truncate: 220 }}</p>
  <a class="btn btn--primary" href="{{ latest.url | relative_url }}">Read the post</a>
</section>
{% endif %}

<section class="trust-strip">
  <span>Iceberg REST compatible</span>
  <span>Tested with DuckDB + Trino</span>
  <span>Apache 2.0 open source</span>
</section>

<h2 id="why-floecat">Why teams use Floecat</h2>

<div class="feature-grid">
  <article class="feature-card">
    <p class="feature-card__eyebrow">Federation</p>
    <h3>One API across catalogs</h3>
    <p>Read metadata from Iceberg and Delta catalogs through one stable API surface.</p>
  </article>
  <article class="feature-card">
    <p class="feature-card__eyebrow">Canonical Model</p>
    <h3>Consistent metadata semantics</h3>
    <p>Normalize heterogeneous metadata into a consistent representation for planners.</p>
  </article>
  <article class="feature-card">
    <p class="feature-card__eyebrow">Planner Statistics</p>
    <h3>Richer cost-based optimization</h3>
    <p>Serve NDV, histograms, MCVs, and more for cost-based optimization.</p>
  </article>
  <article class="feature-card">
    <p class="feature-card__eyebrow">Deterministic Planning</p>
    <h3>Stable snapshot pinning</h3>
    <p>Pin snapshots across catalogs for repeatable planning and stable query behavior.</p>
  </article>
</div>

<h2 id="installation">Install by environment</h2>

<section class="install-switcher" data-install-switcher>
  <div class="install-switcher__tabs" role="tablist" aria-label="Install methods">
    <button type="button" id="install-tab-docker" class="active" data-install-tab="docker" role="tab" aria-selected="true" aria-controls="install-panel-docker" tabindex="0">Docker</button>
    <button type="button" id="install-tab-source" data-install-tab="source" role="tab" aria-selected="false" aria-controls="install-panel-source" tabindex="-1">Source</button>
    <button type="button" id="install-tab-cli" data-install-tab="cli" role="tab" aria-selected="false" aria-controls="install-panel-cli" tabindex="-1">CLI</button>
    <button type="button" id="install-tab-duckdb" data-install-tab="duckdb" role="tab" aria-selected="false" aria-controls="install-panel-duckdb" tabindex="-1">DuckDB</button>
  </div>

  <article id="install-panel-docker" class="install-switcher__panel active" data-install-panel="docker" role="tabpanel" aria-labelledby="install-tab-docker">
    <pre class="install-code"><code>cd docker
FLOECAT_ENV_FILE=./env.localstack COMPOSE_PROFILES=localstack,trino \
  docker compose -f docker-compose.yml up -d --wait</code></pre>
  </article>

  <article id="install-panel-source" class="install-switcher__panel" data-install-panel="source" role="tabpanel" aria-labelledby="install-tab-source" hidden>
    <pre class="install-code"><code>git clone https://github.com/eng-floe/floecat.git
cd floecat
make run</code></pre>
  </article>

  <article id="install-panel-cli" class="install-switcher__panel" data-install-panel="cli" role="tabpanel" aria-labelledby="install-tab-cli" hidden>
    <pre class="install-code"><code>cd docker
FLOECAT_ENV_FILE=./env.localstack docker compose -f docker-compose.yml run --rm cli</code></pre>
  </article>

  <article id="install-panel-duckdb" class="install-switcher__panel" data-install-panel="duckdb" role="tabpanel" aria-labelledby="install-tab-duckdb" hidden>
    <pre class="install-code"><code>duckdb -init tools/duckdb-localstack-init.sql
SELECT * FROM iceberg_floecat.iceberg.orders;</code></pre>
  </article>
</section>

<h2 id="stack">Built for your stack</h2>

<section class="stack-groups">
  <article class="stack-group">
    <h3>Formats</h3>
    <div class="stack-chips">
      <span>Iceberg</span>
      <span>Delta Lake</span>
      <span>Parquet</span>
      <span>Arrow</span>
    </div>
  </article>
  <article class="stack-group">
    <h3>Engines and APIs</h3>
    <div class="stack-chips">
      <span>DuckDB</span>
      <span>Trino</span>
      <span>REST API</span>
      <span>CLI</span>
    </div>
  </article>
  <article class="stack-group">
    <h3>Storage and operations</h3>
    <div class="stack-chips">
      <span>S3 / LocalStack</span>
      <span>Docker</span>
      <span>Telemetry</span>
      <span>Security</span>
    </div>
  </article>
</section>

<h2 id="try-now">Try it now</h2>

<section class="try-now-grid">
  <article class="try-card">
    <h3>Start local stack</h3>
    <p>Run Floecat with LocalStack and Trino in minutes via Docker Compose.</p>
    <a class="btn btn--primary" href="{{ '/documentation/docker/' | relative_url }}">Open local setup</a>
  </article>
  <article class="try-card">
    <h3>Query with DuckDB</h3>
    <p>Attach Iceberg through Floecat and run queries from a local DuckDB session.</p>
    <a class="btn btn--inverse" href="https://github.com/eng-floe/floecat/blob/main/tools/duckdb-localstack-init.sql">Open DuckDB init script</a>
  </article>
  <article class="try-card">
    <h3>Explore APIs</h3>
    <p>Inspect service endpoints and gateway behavior for integration work.</p>
    <a class="btn btn--inverse" href="{{ '/documentation/service/' | relative_url }}">Read API docs</a>
  </article>
</section>

<h2 id="problem">The problem this solves</h2>

Open table formats solved storage interoperability, but not
planner-ready statistics,
cross-catalog semantics,
or cross-engine consistency.

Floecat is built for metadata <strong>consumption</strong>, not mutation:
it gives engines a stable metadata layer
instead of making each engine rebuild catalog logic.

<h2 id="quickstart">Quickstart</h2>

Clone, start the service, and open the CLI:

```bash
git clone https://github.com/eng-floe/floecat.git
cd floecat
make run
make cli-run
```

<section class="quickstart-actions">
  <a class="btn btn--primary" href="{{ '/documentation/docker/' | relative_url }}">Run Local Stack</a>
  <a class="btn btn--inverse" href="{{ '/documentation/architecture/' | relative_url }}">Read Architecture</a>
  <a class="btn btn--inverse" href="{{ '/documentation/iceberg-rest-gateway/' | relative_url }}">Iceberg REST Guide</a>
</section>

Then point Floecat at an Iceberg or Delta catalog and query through its APIs
or from engines like DuckDB and Trino.

<h2 id="contact-contribute">Contact and contribute</h2>

<section class="community-actions">
  <a class="community-card" href="https://github.com/eng-floe/floecat/issues/new/choose">
    <h3>Report an issue</h3>
    <p>Found a bug or docs gap? Open a ticket with reproduction details.</p>
  </a>
  <a class="community-card" href="https://github.com/eng-floe/floecat/discussions">
    <h3>Start a discussion</h3>
    <p>Ask implementation questions or propose roadmap ideas with the team.</p>
  </a>
  <a class="community-card" href="https://github.com/eng-floe/floecat/blob/main/CONTRIBUTING.md">
    <h3>Contribute code/docs</h3>
    <p>Read contribution workflow, conventions, and pull request expectations.</p>
  </a>
</section>

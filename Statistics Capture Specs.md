Statistics Capture Specs

Overview

This document specifies the approach Floe and Floecat will follow when gathering and managing statistics for upstream data sources.

Floecat acts as a catalog unifier and statistics control plane. It provides a unified statistics API across connectors such as Iceberg, Delta Lake, PostgreSQL, and others. Statistics computation is delegated to pluggable statistics providers, while Floecat remains the system of record for persisted statistics.

Statistics are primarily used to improve query planning and optimization in FloeDB, but the statistics service is designed to be generic and reusable across multiple engines and deployments.

This specification currently focuses on Iceberg and Delta tables but is designed to support other connectors.

Two main scenarios are addressed:
	1.	A new table registered with Floecat with no existing statistics
	2.	Changes to upstream data resulting in new snapshots or updated table state

Floecat supports both synchronous statistics capture (just-in-time for queries) and asynchronous statistics capture (background harvesting and reconciliation).

A key design goal is progressive refinement, where statistics improve over time without blocking user queries.

⸻

Architecture

Floecat Statistics Service

Floecat provides a generic statistics service API for:
	•	retrieving statistics
	•	requesting statistics computation
	•	publishing externally computed statistics

Floecat stores statistics and exposes them to planners and engines. Floecat does not require that all statistics be computed internally.

Instead, statistics computation is delegated to statistics providers.

Responsibilities of Floecat

Floecat is responsible for:
	•	statistics storage
	•	statistics versioning
	•	statistics completeness and quality metadata
	•	snapshot association
	•	provider routing
	•	job scheduling
	•	progressive refinement
	•	planner interaction

Floecat does not directly implement all computation logic.

⸻

Statistics Providers

Statistics computation is performed by statistics providers registered with Floecat through a provider SPI.

A provider may compute statistics using:
	•	metadata-only techniques
	•	source-native facilities
	•	FloeDB-managed compute
	•	external compute engines

Examples of providers include:

Provider	Description
Iceberg metadata provider	derives statistics from Iceberg manifests
Delta metadata provider	derives statistics from Delta transaction logs
FloeDB engine provider	computes statistics using FloeDB execution engine
External provider	external system computing statistics and publishing results

Providers advertise capabilities such as:
	•	supported connectors
	•	supported statistics metrics
	•	snapshot awareness
	•	expression support
	•	synchronous support
	•	asynchronous support

Floecat selects providers dynamically depending on:
	•	requested statistics
	•	connector type
	•	execution mode (sync vs async)
	•	cost and latency constraints

## Provider SPI and Routing

Floecat uses a dedicated statistics provider SPI to decouple orchestration from computation.

Each provider must advertise capabilities including:
- supported connectors
- supported target types (table, column, expression)
- supported statistic kinds
- snapshot awareness
- execution modes (sync, async)
- sampling support

Floecat selects providers dynamically using a routing layer based on:
- requested statistics
- target type
- execution mode (sync vs async)
- latency budget
- configured provider priority
- availability of metadata or sampling paths

Multiple providers may be used for a single request, with preference given to lower-cost or metadata-derived approaches before compute-heavy estimation.

⸻

Statistics Model

Floecat stores statistics centrally. FloeDB and other consumers retrieve statistics through the Floecat API.

Statistics are associated with:
	•	upstream object
	•	snapshot or version identity
	•	statistics target (table, column, expression)

## Canonical Expression Identity

Expression-based statistics targets must have a canonical identity so they can be stored, reused, and matched reliably across requests.

Each expression target is defined by:
- a normalized expression intermediate representation (IR)
- a stable hash derived from the normalized IR
- the result type of the expression
- normalized variant path semantics where applicable

Normalization rules must ensure that semantically equivalent expressions map to the same identity. This identity is used as part of the storage key for statistics and must be stable across planner requests, provider execution, and persistence.

## Statistics Targets

Statistics may apply to:
	•	tables
	•	columns
	•	expressions

Expressions include:
	•	scalar expressions over columns
	•	functions
	•	derived fields
	•	shredded fields from variant columns

## Statistics Storage Key

Statistics are uniquely identified by the tuple:
- table identifier
- snapshot identifier
- statistics target (including expression identity)

This ensures that statistics are:
- snapshot-consistent
- reusable across queries
- correctly scoped to expression-derived values

⸻

Variant and Shredded Columns

Many modern data models include variant or semi-structured columns (JSON, struct, map, etc).

FloeDB frequently shreds variant data into virtual columns during query execution.

Statistics support must therefore extend beyond base columns to derived paths within variant data.

Examples:

payload:user.id
payload:event.type
payload:items[0].product_id

These fields are treated as statistics targets equivalent to regular columns.

Statistics may include:
	•	NDV
	•	null count
	•	min/max
	•	histogram
	•	most common values

Shredded fields may be materialized as virtual columns within the catalog metadata.

Statistics providers may compute statistics for these fields by:
	•	evaluating extraction expressions
	•	using schema hints
	•	sampling decoded records

Variant field statistics are critical for:
	•	predicate selectivity
	•	join cardinality
	•	grouping estimates
	•	adaptive shredding

Variant semantics must explicitly distinguish:
- missing values (path not present)
- null values (explicit null)
- extraction or type errors

These distinctions must be reflected in both execution and statistics, including null counts and value distributions.

⸻

Statistics Versioning

Statistics are versioned according to upstream table state.

For Iceberg and Delta this corresponds to:
	•	Iceberg snapshot id
	•	Delta version

Statistics must correspond to a specific upstream snapshot so that:
	•	time travel queries use correct statistics
	•	query plans remain reproducible
	•	statistics remain consistent with data state

⸻

Statistics Types

Table Statistics
	•	row count
	•	file count
	•	total bytes
	•	snapshot id
	•	snapshot timestamp

Column Statistics
	•	min value
	•	max value
	•	null count
	•	NDV estimate
	•	most common values
	•	histogram
	•	average value size

Expression Statistics
	•	NDV
	•	histogram
	•	null count
	•	min/max
	•	value distribution

Expression statistics are particularly important for:
	•	variant shredding
	•	join expressions
	•	derived filter predicates

⸻

Statistics Provenance

Each statistic records metadata describing how it was obtained.

Possible provenance values:

Provenance	Meaning
SOURCE_NATIVE	obtained directly from upstream metadata
UPSTREAM_METADATA_DERIVED	derived from file or table metadata
SYNC_ESTIMATED	computed during synchronous request
ASYNC_RECONCILED	computed during asynchronous reconciliation
FLOEDB_COMPUTE	computed using FloeDB execution engine
EXTERNAL_PROVIDER	computed by external provider

Additional metadata may include:
	•	sampling strategy
	•	estimator type
	•	bytes scanned
	•	row groups scanned
	•	compute timestamp

This allows the planner to reason about statistics quality and reliability.

In addition to provenance, each statistic must include:

- completeness: COMPLETE, PARTIAL, or UNAVAILABLE
- confidence level for estimated values
- coverage metrics (rows scanned, files scanned, row groups sampled)
- timestamps for capture and last refresh

This metadata allows the planner to reason about the reliability and applicability of statistics.

⸻

Synchronous Statistics Capture

For a given query, the planner requests statistics from Floecat synchronously.

If statistics are not already stored, Floecat may attempt to generate them within a strict latency budget.

Latency Budget

Synchronous capture must complete within 1 second.

Within this budget Floecat may:
	•	return existing persisted statistics
	•	derive metadata statistics
	•	compute sampled statistics via providers

If the deadline is exceeded, Floecat returns partial or no statistics.

⸻

Statistics Resolution Flow

When the planner requests statistics, Floecat follows this resolution process:

1. Resolve the target snapshot (current or time-travel).
2. Load any persisted statistics for the requested targets.
3. Identify missing, incomplete, stale, or low-confidence statistics.
4. If allowed, submit a scoped synchronous capture request via the reconciler.
5. Execute under the latency budget (~1 second).
6. Return the best available statistics (including partial results).
7. Persist any valid results produced during synchronous execution.
8. If results are partial or weak, enqueue an asynchronous reconcile job.

This flow ensures that user-facing queries are not blocked while still improving statistics over time.

⸻

Sync Job Scheduling

Synchronous statistics requests are inserted at the head of the durable job queue.

Async jobs may be preempted to prioritize synchronous work.

If a synchronous request waits longer than the time budget, Floecat returns immediately.

⸻

Sync Results

Responses include metadata describing:
	•	completeness
	•	provenance
	•	snapshot id
	•	whether stats were persisted or generated on demand

If partial statistics are returned, an async reconcile job is automatically scheduled.

⸻

Asynchronous Statistics Capture

Async capture runs continuously in the background.

Async jobs perform:
	•	metadata reconciliation
	•	deeper sampling
	•	full column statistics
	•	expression statistics
	•	variant field statistics

Async jobs improve statistics coverage and completeness over time.

⸻

Triggering Async Reconciliation

Async jobs are triggered when:
	•	a new table is registered
	•	no statistics exist
	•	new snapshot detected
	•	sync request returned partial stats
	•	administrator explicitly requests refresh

Async refresh may also be triggered by data change thresholds.

Example threshold:

abs(current_row_count - previous_row_count) >
max(0.10 * previous_row_count, sqrt(previous_row_count))

This avoids recomputing statistics unnecessarily.

⸻

Async Prioritization

Async jobs prioritize:
	1.	smaller tables first
	2.	frequently queried tables
	3.	missing statistics
	4.	important columns

Priority columns include:
	•	join keys
	•	filter columns
	•	grouping columns
	•	variant fields referenced by queries

At most 30 columns may be processed per pass.

⸻

Planner Interaction

The planner interacts with Floecat through the statistics API.

For each query the planner may request statistics for:
	•	tables
	•	columns
	•	expressions
	•	shredded variant fields

The planner also informs Floecat which statistics are most valuable.

Floecat returns the best available statistics within the latency budget.

Planner requests and responses must support expression-based targets. Column-based APIs may be preserved for compatibility but must internally map to the generalized target model.

The response payload must include:
- target identity
- snapshot identifier
- statistic values
- completeness
- provenance
- confidence and coverage metadata

⸻

Statistics Quality Levels

Statistics may be:
	•	COMPLETE
	•	PARTIAL
	•	UNAVAILABLE

Additional confidence levels may be attached to estimated values such as NDV.

⸻

Operational Model

Async statistics computation runs on background worker capacity.

Compute may run on:
	•	EC2 spot instances
	•	container workers
	•	lambda functions
	•	registered external compute providers

Workers must support:
	•	preemption by synchronous jobs
	•	failure retries
	•	elastic scaling

The job queue must be durable and prioritized.

⸻

NDV Estimation

When NDV statistics are unavailable Floecat estimates NDV using sampling.

Floecat evaluates multiple estimators simultaneously and selects the most reliable result.

The three estimators are:

Linear extrapolation

Distinct values observed in the sample are scaled according to sample fraction.

Best for high-cardinality columns.

⸻

Frequency-of-frequencies estimator

Uses value frequency distribution to estimate unseen values.

Best for moderate-cardinality columns.

⸻

Discovery curve estimator

Tracks how fast new distinct values appear as more rows are scanned.

When discovery slows the estimator predicts NDV convergence.

⸻

Row Group Sampling

Floecat samples Parquet row groups rather than scanning entire tables.

Two sampling strategies are supported.

Random sampling

Row groups are selected randomly.

Provides unbiased estimates.

Metadata-guided sampling

Row groups are selected using metadata ranges to maximize value coverage.

Useful for high-cardinality columns.

Sampling diagnostics are recorded to evaluate estimator reliability.

⸻

Estimate Selection

Candidate NDV estimates are compared.

Estimates that violate stability diagnostics are discarded.

Remaining estimates are combined to produce a final NDV and confidence level.

This ensemble approach provides robust NDV estimates even with limited sampling.

⸻

Failure Handling

If synchronous capture cannot produce useful statistics within the time budget, Floecat returns partial or no statistics rather than delaying query execution.

Any valid partial results should still be persisted.

Asynchronous failures should be retried according to policy and surfaced operationally. Failures for one table or target must not block progress for others.

Snapshot consistency must be preserved in all failure scenarios. Statistics from a different snapshot must not be used unless explicitly marked as approximate or fallback.

⸻

## Implementation Plan

This section translates the design into an incremental implementation plan intended for two engineers working in parallel. The plan is organized as a sequence of small, reviewable pull requests. Each PR should leave the system in a working state, preserve backward compatibility until the explicit migration step, and add enough scaffolding for the next PR to build on it.

### Implementation Principles

The implementation should follow these principles:
- each task should correspond to one small PR where possible
- each PR should be incremental and independently testable
- existing column-based planner and storage paths should keep working until the new expression-target path is fully integrated
- sync and async behavior should be introduced behind feature flags or guarded policy checks where practical
- expression identity, provider routing, and planner payload changes should be introduced in layers rather than all at once

### Team Split

#### Engineer A: Floecat Control Plane / Planner / Storage / Orchestration

Engineer A owns all Floecat-side work, including the planner-facing, catalog-facing, storage, and orchestration layers:
- planner API evolution and compatibility layer
- persisted stats data model and repository changes
- provenance, quality, completeness, and coverage fields on stored and returned stats
- target-aware service-layer resolution
- sync request/response behavior and bounded query-time orchestration
- async orchestration policy, queue semantics, deduplication, retries, and scheduling integration
- service-layer integration, rollout, migration, and backward compatibility
- integration of the stats library into Floecat provider execution paths

#### Engineer B: Stats Library / Compute Runtime

Engineer B owns the reusable statistics computation library and runtime that Floecat calls into, but does not own the Floecat-side APIs, storage model, planner contract, or queue/orchestration logic.

Engineer B’s responsibilities are limited to:
- canonical expression IR and stable hashing implementation inside the stats library
- expression evaluation support needed for statistics computation
- sampling implementation and scan controls
- NDV estimation framework and diagnostics
- variant extraction semantics in the stats library runtime
- compute telemetry emitted by the stats library
- compute-facing request and response contracts between Floecat and the stats library

Primary files and components:
- stats library codebase
- expression normalization / hashing modules
- sampling and NDV modules
- variant extraction and evaluation logic
- compute telemetry emitted from the stats runtime

### Shared Ownership

The following items must be jointly designed and reviewed before implementation proceeds too far:
- canonical expression identity rules
- compute request/response contract between Floecat and the stats library
- missing vs null vs extraction-error semantics for expression evaluation
- snapshot consistency invariants
- golden end-to-end test matrix
- sync latency SLO validation criteria


### Incremental PR Plan

### PR Summary Table

| PR | Title | Owner | Estimate | Primary dependency |
|---|---|---|---|---|
| PR1 | Add statistics metadata scaffolding | Engineer A | 2 to 3 days | None |
| PR2 | Introduce canonical expression reference model | Engineer B | 3 to 4 days | None |
| PR3 | Add target-aware stats storage model | Engineer A | 3 to 4 days | PR2 |
| PR4 | Extend planner payloads for target-based requests | Engineer A | 3 to 4 days | PR2, PR3 |
| PR5 | Introduce statistics provider SPI and registry | Engineer A | 4 to 5 days | PR1 |
| PR6 | Add source-native provider adapters for Iceberg and Delta | Engineer A | 4 to 5 days | PR5 |
| PR7 | Add compute provider skeleton for arbitrary expressions | Engineer B | 4 to 5 days | PR2 |
| PR8 | Implement variant-path semantics through expression evaluation | Engineer B | 4 to 6 days | PR2, PR7 |
| PR9 | Wire target-aware resolution in service layer | Engineer A | 3 to 4 days | PR3, PR4, PR5 |
| PR10 | Add bounded synchronous compute-on-miss path | Engineer A | 4 to 5 days | PR7, PR9 |
| PR11 | Add async follow-up scheduling and durable queue policy | Engineer A | 3 to 4 days | PR10 |
| PR12 | Add async prioritization policy | Engineer A | 3 to 4 days | PR11 |
| PR13 | Add NDV estimator orchestration and diagnostics | Engineer B | 5 to 6 days | PR7 |
| PR14 | Add sampling policy controls and telemetry | Engineer B | 3 to 4 days | PR7 |
| PR15 | Add planner adoption path for target-based optimization | Engineer A | 3 to 5 days | PR9, PR10 |
| PR16 | Migration, rollout, and cleanup | Engineer A | 4 to 5 days | PR12, PR15 |

Notes:
- Dependencies indicate the first PRs that should land before a later PR is considered ready to merge.
- PR13 and PR14 can proceed in parallel after the stats library compute entry points in PR7 are stable.
- PR10 is the first PR that requires the full Floecat-to-stats-library integration path to be working end to end.
- PR16 should only begin after the new planner path, sync path, and async follow-up loop are stable enough to roll out safely.


#### PR1. Add statistics metadata scaffolding

Owner: Engineer A  
Estimated time: 2 to 3 days

Goal:
Introduce the new metadata model without changing the planner contract yet.

Scope:
- extend `stats.proto` with explicit provenance, completeness, and confidence-related fields
- add coverage-oriented fields such as bytes scanned, files scanned, row groups sampled, capture timestamp, and refresh timestamp
- update repository model objects to carry the additional metadata
- preserve backward compatibility for all existing readers and writers

Why first:
This creates the result envelope needed by later sync/async and provider work without forcing the expression-target transition immediately.

Acceptance criteria:
- existing stats reads and writes continue to work
- new metadata fields can be persisted and returned
- no planner API changes required yet

#### PR2. Introduce canonical expression reference model

Owner: Engineer B  
Estimated time: 3 to 4 days

Goal:
Create a canonical expression representation that can serve as the identity for all non-table statistics targets.

Scope:
- define normalized expression IR structure
- define stable hashing or fingerprinting rules
- define type semantics for expression results
- define canonical mapping from a plain column reference into expression form
- define normalization rules for variant path extraction expressions

Why now:
All later target-based routing, storage, and planner integration depend on stable expression identity.

Acceptance criteria:
- equivalent expressions normalize to the same identity
- different expressions do not collide under expected cases
- column references can be represented as expressions without changing existing behavior
- basic unit tests cover normalization and hashing

#### PR3. Add target-aware stats storage model

Owner: Engineer A  
Estimated time: 3 to 4 days

Goal:
Extend persisted stats keying from column-based identity to target-based identity while keeping old paths intact.

Scope:
- add a generalized target identity to persisted stats records
- support keying by `(table_id, snapshot_id, target_id)`
- keep legacy column-based key lookup working through compatibility mapping
- update `StatsRepository.java` and related model classes
- add migration-safe read/write behavior

Why here:
Once canonical expression identity exists, storage can begin accepting it without yet forcing planner adoption.

Acceptance criteria:
- stats can be written and read by target identity
- old column-based lookups still work
- target-based records do not break existing snapshots or queries

#### PR4. Extend planner payloads for target-based requests

Owner: Engineer A  
Estimated time: 3 to 4 days

Goal:
Allow planner-facing requests and responses to carry generalized targets while preserving compatibility with current column-id callers.

Scope:
- extend `planner_stats_bundle.proto` to accept target descriptors in addition to current column identifiers
- include provenance, completeness, confidence, and coverage metadata in planner responses
- update `PlannerStatsBundleService.java` to map old column requests to canonical expression targets internally
- preserve the existing planner contract during the transition

Why here:
This unlocks end-to-end target-based reads while avoiding a flag day migration.

Acceptance criteria:
- existing column-based planner requests still function
- new target-based requests can be issued and resolved
- planner responses include the new metadata envelope

#### PR5. Introduce statistics provider SPI and registry

Owner: Engineer A  
Estimated time: 4 to 5 days

Goal:
Add the Floecat-side provider SPI, capability model, and routing layer that can call native metadata paths or the external stats library.

Scope:
  - define Floecat-side provider interfaces for capability discovery and stats computation
  - define capability descriptors for connector support, target support, statistic kinds, sync/async support, and sampling support
  - add registry and routing skeleton inside Floecat
  - wrap one baseline existing Floecat computation or metadata path behind the new SPI

Why here:
This creates the extensibility layer needed before native and compute-backed providers can be plugged in cleanly.

Acceptance criteria:
  - at least one provider can be discovered through the registry
  - routing can choose a provider for a simple request
  - no regression to existing stats retrieval paths

#### PR6. Add source-native provider adapters for Iceberg and Delta

Owner: Engineer A  
Estimated time: 4 to 5 days

Goal:
Expose existing native metadata-driven paths through the new statistics provider SPI.

Scope:
  - adapt Iceberg metadata stats into provider form
  - adapt Delta metadata stats into provider form where upstream shape permits
  - mark unsupported deep expression or variant cases as requiring compute fallback
  - emit provenance as `SOURCE_NATIVE` or `UPSTREAM_METADATA_DERIVED`

Why here:
This provides immediate value through the new provider layer before compute-heavy expression support lands.

Acceptance criteria:
  - Iceberg and Delta native stats can be resolved through the provider router
  - unsupported targets fail clearly or fall through to later compute providers
  - provenance is set correctly for native metadata results

#### PR7. Add compute provider skeleton for arbitrary expressions

Owner: Engineer B  
Estimated time: 4 to 5 days

Goal:
Create the stats library-side compute entry points needed to evaluate expression targets beyond what native metadata can provide.

Scope:
  - define stats library request contract for required columns, expression evaluation, and sampling policy
  - implement library entry points for expression-target computation
  - support basic expression metrics first, such as null count and NDV hooks
  - emit compute telemetry into the library response metadata

Why here:
This is the foundation for expression-target statistics and for treating variant paths as ordinary expressions.

Acceptance criteria:
  - compute provider can accept expression targets
  - provider produces at least one useful metric for expression targets
  - telemetry fields are populated in returned metadata

#### PR8. Implement variant-path semantics through expression evaluation

Owner: Engineer B  
Estimated time: 4 to 6 days

Goal:
Handle variant and shredded-column use cases by treating path extraction as ordinary expression evaluation with explicit semantics.

Scope:
  - define path grammar and canonicalization rules for extraction expressions
  - implement missing vs null vs extraction-error accounting
  - make result-type-dependent capabilities explicit, especially optional min/max support
  - keep raw variant values non-orderable by default while allowing orderable extracted scalar results where valid

Why here:
This removes most special casing and makes variant support consistent with the expression-target model.

Acceptance criteria:
  - variant extraction expressions have stable canonical identity
  - statistics distinguish missing, null, and error outcomes
  - invalid min/max assumptions are avoided for non-orderable results

#### PR9. Wire target-aware resolution in service layer

Owner: Engineer A  
Estimated time: 3 to 4 days

Goal:
Connect planner requests, repository lookups, and provider routing into a unified resolution pipeline.

Scope:
  - update `TableStatisticsServiceImpl.java` to resolve by target identity
  - implement the documented resolution flow: load persisted stats, identify gaps, invoke providers when needed, return best available results
  - support partial results in the response model
  - preserve snapshot consistency checks

Why here:
This creates the first true end-to-end target-based flow without yet introducing sync budget enforcement.

Acceptance criteria:
  - planner request can resolve target-based stats end to end
  - persisted stats are preferred when present
  - provider routing is used for missing stats
  - snapshot identity is preserved throughout

#### PR10. Add bounded synchronous compute-on-miss path

Owner: Engineer A  
Estimated time: 4 to 5 days

Goal:
Implement the 1-second planner-facing synchronous capture policy.

Scope:
  - wire the Floecat service layer into the reconciler sync path and the stats library compute entry point where needed
  - enforce an approximate 1-second deadline for synchronous resolution
  - prefer already persisted and lower-cost provider results first
  - return partial or no stats when the budget is exhausted
  - persist any valid partial outputs

Why here:
This is the first user-facing realization of just-in-time statistics capture.

Acceptance criteria:
  - sync capture respects the time budget
  - partial results are possible and correctly annotated
  - persisted partial results survive for later reuse
  - query-facing path does not block indefinitely

#### PR11. Add async follow-up scheduling and durable queue policy

Owner: Engineer A  
Estimated time: 3 to 4 days

Goal:
Ensure weak or partial synchronous results lead to durable async reconciliation.

Scope:
  - enqueue async reconcile work when sync results are partial, incomplete, or low confidence
  - encode trigger reason in queue payload
  - define deduplication and requeue behavior
  - preserve priority ordering of sync above async work
  - update `ReconcilePlannerScheduler.java` and `DurableReconcileJobStore.java`

Why here:
This completes the progressive-refinement loop described in the spec.

Acceptance criteria:
  - sync partial results schedule async follow-up jobs
  - queue deduplication prevents unnecessary duplicate work
  - sync work remains higher priority than async work

#### PR12. Add async prioritization policy

Owner: Engineer A  
Estimated time: 3 to 4 days

Goal:
Implement selective background refresh instead of unconditional recomputation.

Scope:
  - add policies for new table, no stats, partial sync, and manual refresh triggers
  - implement material-change threshold checks
  - prioritize small tables, frequently queried tables, and important targets
  - add deterministic target selection for capped passes such as first 30 targets

Why here:
This makes the async system scalable and aligned with the stated operational goals.

Acceptance criteria:
  - refresh triggers follow documented policy
  - prioritization is deterministic and observable
  - large-table work is bounded and selective

#### PR13. Add NDV estimator orchestration and diagnostics

Owner: Engineer B  
Estimated time: 5 to 6 days

Goal:
Turn the NDV design into a pluggable multi-estimator framework.

Scope:
  - integrate linear extrapolation, frequency-of-frequencies, and discovery-curve estimators behind a common abstraction
  - add sampling diagnostics and estimator selection logic
  - add confidence scoring and rejection of unstable estimates
  - emit estimator type and diagnostics into metadata

Why here:
This upgrades expression and column NDV from a single-path estimate to the design described in the spec.

Acceptance criteria:
  - multiple estimators can run for one request
  - unstable estimates can be rejected
  - selected NDV includes confidence and diagnostics metadata

#### PR14. Add sampling policy controls and telemetry

Owner: Engineer B  
Estimated time: 3 to 4 days

Goal:
Make sampling behavior explicit and observable.

Scope:
  - support random row-group sampling
  - support metadata-guided row-group sampling
  - add configurable limits appropriate for sync vs async contexts
  - emit rows/files/row-groups sampled and bytes scanned into metadata

Why here:
This makes the compute provider operationally debuggable and gives the planner better quality signals.

Acceptance criteria:
  - sampling strategy is visible in output metadata
  - sync and async contexts can apply different sampling limits
  - compute telemetry is exposed consistently

#### PR15. Add planner adoption path for target-based optimization

Owner: Engineer A  
Estimated time: 3 to 5 days

Goal:
Move planner usage from compatibility mapping toward explicit target-based requests.

Scope:
  - add support for planner-generated expression targets
  - use completeness and confidence metadata in planner decision-making where practical
  - keep compatibility path for legacy callers until rollout is complete

Why here:
This is where the new model starts delivering real planning value beyond column-only statistics.

Acceptance criteria:
  - planner can request at least some expression targets directly
  - planner can observe completeness/confidence fields
  - legacy paths still function during rollout

#### PR16. Migration, rollout, and cleanup

Owner: Engineer A  
Estimated time: 4 to 5 days

Goal:
Complete the transition safely and document the operational rollout plan.

Scope:
  - define feature flags and staged enablement order
  - add dual-read or compatibility behavior where required
  - add migration or backfill plan for persisted stats where needed
  - document rollback behavior
  - clean up temporary compatibility code only if rollout conditions are satisfied

Why last:
Migration and cleanup should happen only after the new end-to-end flow is stable and validated.

Acceptance criteria:
  - rollout can be staged safely
  - old and new paths can coexist during transition
  - migration plan is explicit and testable

### Suggested Delivery Cadence

A practical delivery cadence for two engineers is:

#### Week 1
- PR1: statistics metadata scaffolding
- PR2: canonical expression reference model
- joint design review for shared semantics and test plan

#### Week 2
- PR3: target-aware stats storage model
- PR5: Floecat-side provider SPI and registry

#### Week 3
- PR4: planner payload extension
- PR6: Floecat-side source-native provider adapters
- PR7: stats library compute entry points

#### Week 4
- PR8: variant-path semantics
- PR9: target-aware service-layer resolution
- begin integration testing across planner, storage, and providers

#### Week 5
- PR10: bounded synchronous compute-on-miss
- PR11: async follow-up scheduling

#### Week 6
- PR12: async prioritization policy
- PR13: NDV orchestration and diagnostics

#### Week 7
- PR14: sampling controls and telemetry
- PR15: planner adoption for explicit target-based usage

#### Week 8
- PR16: migration, rollout, cleanup, and hardening
- performance tuning, observability validation, and final documentation

### End-to-End Definition of Done

The implementation should be considered complete when all of the following are true:
- planner requests can resolve statistics by canonical target identity, including ordinary column expressions and selected variant extraction expressions
- persisted statistics are keyed by target identity and snapshot identity
- Floecat-side provider routing supports both source-native execution and compute-backed execution via the stats library
- synchronous resolution can return bounded partial results within the user-facing budget
- partial or weak synchronous results trigger durable asynchronous reconciliation
- provenance, completeness, confidence, and coverage metadata are persisted and returned end to end
- variant extraction semantics are stable and tested for missing, null, and error outcomes
- backward compatibility for existing column-based planner paths has been maintained through migration
- rollout and rollback procedures are documented and validated

### Test and Validation Requirements

At minimum, each PR should add or update tests appropriate to its scope. Across the full implementation, the following coverage is required:
- unit tests for expression normalization and hashing
- repository tests for target-based persistence
- planner compatibility tests for column-id and target-based requests
- Floecat-side provider routing tests and Floecat-to-stats-library integration tests
- sync budget behavior tests
- async queue and deduplication tests
- NDV estimator comparison and confidence tests
- variant semantics tests covering missing, null, error, and typed scalar extraction
- snapshot consistency tests including time-travel requests
- end-to-end golden tests from planner request through persistence and response

Summary

The Floecat statistics system provides:
	•	a generic statistics service API
	•	pluggable statistics providers
	•	snapshot-consistent statistics
	•	synchronous just-in-time capture
	•	asynchronous background refinement
	•	statistics for columns, expressions, and variant fields

This architecture allows Floecat to unify statistics across heterogeneous data sources while allowing engines such as FloeDB to plug in their own computation capabilities.

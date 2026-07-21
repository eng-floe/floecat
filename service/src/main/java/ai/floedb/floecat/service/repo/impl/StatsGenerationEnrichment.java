/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.SketchPayload;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import java.util.HashSet;
import java.util.Set;

/**
 * Write-side half of the "generations only enrich" contract: a new stats generation for a snapshot
 * must not lose sketch payloads the superseded generation already held for the same target.
 *
 * <p>Why this exists: stats writers have unequal capabilities. A sampled ANALYZE capture publishes
 * column records rich with sketch payloads (quantiles, MCV, tuple), while a file-group rollup can
 * only derive scalars and merged theta NDV from file records. When the rollup republishes a
 * snapshot's stats it would otherwise supersede a sketch-bearing generation with a scalar-only one
 * — and since the snapshot's data has not changed, every payload of the older generation is still a
 * valid fact for the new one. This class folds those payloads forward at publish time so the newest
 * generation of a snapshot is capability-wise a superset of the one it replaces.
 *
 * <p>Boundaries (deliberate):
 *
 * <ul>
 *   <li><b>Payload enrichment only, never target resurrection.</b> A target the new generation does
 *       not publish stays absent — {@code replaceAllStatsForSnapshot} owns which targets exist (a
 *       republish may legitimately drop a column). Readers cover absent targets through the
 *       pinned/newest generation fallback.
 *   <li><b>Add, never overwrite.</b> The incoming record's own fields and payloads always win; a
 *       superseded payload is folded in only when the incoming record has no payload with the same
 *       (role, sketch_type) identity — the same identity the serving path matches on.
 *   <li><b>Sketches only.</b> Scalar fields (row counts, min/max, NDV estimates) come from the new
 *       capture and are authoritative; only immutable sketch payloads are carried forward.
 * </ul>
 */
final class StatsGenerationEnrichment {
  private StatsGenerationEnrichment() {}

  /**
   * Returns {@code incoming} with every sketch payload of {@code previous} folded in whose (role,
   * sketch_type) identity the incoming record does not already carry. Records without scalar stats
   * on either side are returned unchanged — only column-style records carry sketch payloads.
   */
  static TargetStatsRecord carrySketchesForward(
      TargetStatsRecord incoming, TargetStatsRecord previous) {
    if (!incoming.hasScalar() || !previous.hasScalar()) {
      return incoming;
    }
    ScalarStats incomingScalar = incoming.getScalar();
    ScalarStats previousScalar = previous.getScalar();

    // Identities the incoming record already serves, across BOTH payload locations: the serving
    // path (findMatchingSketch) searches scalar.sketches and scalar.ndv.sketches alike, so a
    // payload present in either place must suppress the carry-forward of its identity.
    Set<String> present = new HashSet<>();
    incomingScalar.getSketchesList().forEach(sketch -> present.add(identity(sketch)));
    if (incomingScalar.hasNdv()) {
      incomingScalar.getNdv().getSketchesList().forEach(sketch -> present.add(identity(sketch)));
    }

    ScalarStats.Builder enriched = null;
    for (SketchPayload sketch : previousScalar.getSketchesList()) {
      if (present.add(identity(sketch))) {
        enriched = enriched == null ? incomingScalar.toBuilder() : enriched;
        enriched.addSketches(sketch);
      }
    }
    if (previousScalar.hasNdv()) {
      Ndv.Builder ndv = null;
      for (SketchPayload sketch : previousScalar.getNdv().getSketchesList()) {
        if (present.add(identity(sketch))) {
          if (ndv == null) {
            enriched = enriched == null ? incomingScalar.toBuilder() : enriched;
            // Carry the payload into the incoming NDV envelope (creating an estimate-less one if
            // the incoming record has none): the new capture's NDV ESTIMATE stays authoritative,
            // only the superseded payload rides along. A fabricated envelope has hasNdv() == true
            // with the exact/approx mode oneof UNSET — consumers must gate estimates on the mode,
            // never on hasNdv() alone (the serving path already does).
            ndv = enriched.hasNdv() ? enriched.getNdv().toBuilder() : Ndv.newBuilder();
          }
          ndv.addSketches(sketch);
        }
      }
      if (ndv != null) {
        enriched.setNdv(ndv);
      }
    }
    if (enriched == null) {
      return incoming;
    }
    return incoming.toBuilder().setScalar(enriched).build();
  }

  /**
   * Payload identity for the carry-forward decision: role + sketch_type, mirroring the serving
   * path's match rule so "already present" here means exactly "already servable" there.
   */
  private static String identity(SketchPayload sketch) {
    return sketch.getRole().getNumber() + "|" + sketch.getSketchType();
  }
}

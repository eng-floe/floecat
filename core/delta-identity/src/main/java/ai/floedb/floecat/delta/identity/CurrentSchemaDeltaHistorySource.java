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

package ai.floedb.floecat.delta.identity;

import ai.floedb.floecat.schema.identity.HistoryTier;
import io.delta.kernel.Snapshot;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** A Delta source for catalogs that expose a current schema but no ordered transaction history. */
public final class CurrentSchemaDeltaHistorySource implements DeltaHistorySource {
  private final String tableName;
  private final HistoryTier historyTier;
  private final long sourceVersion;
  private final boolean sourceVersionAnchored;
  private final Snapshot snapshot;

  public CurrentSchemaDeltaHistorySource(
      String tableName,
      String tablePath,
      long sourceVersion,
      boolean sourceVersionAnchored,
      String schemaJson,
      Map<String, String> properties,
      List<String> partitionColumns,
      HistoryTier historyTier) {
    this.tableName = Objects.requireNonNull(tableName, "tableName");
    if (Objects.requireNonNull(historyTier, "historyTier").provesContinuity()) {
      throw new IllegalArgumentException("Current-schema sources cannot prove ordered history");
    }
    this.historyTier = historyTier;
    this.sourceVersion = sourceVersion;
    this.sourceVersionAnchored = sourceVersionAnchored;
    this.snapshot =
        KernelAdapter.currentSchemaSnapshot(
            Objects.requireNonNullElse(tablePath, ""),
            sourceVersion,
            Objects.requireNonNull(schemaJson, "schemaJson"),
            Objects.requireNonNull(properties, "properties"),
            Objects.requireNonNull(partitionColumns, "partitionColumns"));
  }

  @Override
  public String tableName() {
    return tableName;
  }

  @Override
  public long headVersion() {
    return sourceVersion;
  }

  @Override
  public HistoryTier historyTier() {
    return historyTier;
  }

  @Override
  public boolean sourceVersionAnchored() {
    return sourceVersionAnchored;
  }

  @Override
  public Optional<Snapshot> snapshot(long version) {
    return version == sourceVersion ? Optional.of(snapshot) : Optional.empty();
  }

  @Override
  public MetadataHistory metadataVersions(long firstVersion, long lastVersion) {
    return MetadataHistory.gap("The catalog exposes no ordered Delta metadata history");
  }
}

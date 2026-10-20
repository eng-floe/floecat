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
import java.util.Objects;
import java.util.Optional;

/** Current catalog schema without evidence of ordered Delta transaction history. */
public final class CurrentSchemaDeltaHistorySource implements DeltaHistorySource {
  private final String tableName;
  private final DeltaIdentitySnapshot snapshot;
  private final boolean sourceVersionAnchored;

  public CurrentSchemaDeltaHistorySource(
      String tableName, DeltaIdentitySnapshot snapshot, boolean sourceVersionAnchored) {
    this.tableName = Objects.requireNonNull(tableName, "tableName");
    this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
    this.sourceVersionAnchored = sourceVersionAnchored;
  }

  @Override
  public String tableName() {
    return tableName;
  }

  @Override
  public long headVersion() {
    return snapshot.version();
  }

  @Override
  public HistoryTier historyTier() {
    return HistoryTier.NONE;
  }

  @Override
  public boolean sourceVersionAnchored() {
    return sourceVersionAnchored;
  }

  @Override
  public Optional<DeltaIdentitySnapshot> snapshot(long version) {
    return version == snapshot.version() ? Optional.of(snapshot) : Optional.empty();
  }

  @Override
  public MetadataHistory metadataVersions(long firstVersion, long lastVersion) {
    return MetadataHistory.gap("The catalog exposes no ordered Delta metadata history");
  }
}

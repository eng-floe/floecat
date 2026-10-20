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
package ai.floedb.floecat.catalog.sharing;

import ai.floedb.floecat.client.sharing.DeltaSharingModel.TableDescription;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TableHistory;
import ai.floedb.floecat.delta.identity.DeltaHistorySource;
import ai.floedb.floecat.delta.identity.DeltaIdentitySnapshot;
import ai.floedb.floecat.schema.identity.HistoryTier;
import java.util.List;
import java.util.Optional;

/** Current schema plus unversioned query history, sufficient to recognize a data-only range. */
final class DeltaSharingQueryHistorySource implements DeltaHistorySource {
  private final String tableName;
  private final long headVersion;
  private final DeltaIdentitySnapshot head;
  private final boolean metadataChanged;
  private final UnorderedHistory unorderedHistory;

  DeltaSharingQueryHistorySource(
      String tableName, long headVersion, TableDescription current, TableHistory history) {
    this.tableName = tableName;
    this.headVersion = headVersion;
    this.metadataChanged = !history.metadata().isEmpty() || !history.protocols().isEmpty();
    this.unorderedHistory =
        new UnorderedHistory(
            history.metadata().stream().map(DeltaSharingIdentityActions::metadata).toList(),
            history.protocols().stream()
                .map(historical -> historical.protocol())
                .map(DeltaSharingIdentityActions::protocol)
                .toList());
    this.head =
        DeltaSharingIdentityActions.snapshot(headVersion, current.metadata(), current.protocol());
  }

  @Override
  public String tableName() {
    return tableName;
  }

  @Override
  public long headVersion() {
    return headVersion;
  }

  @Override
  public HistoryTier historyTier() {
    return HistoryTier.UNORDERED_QUERY_HISTORY;
  }

  @Override
  public Optional<DeltaIdentitySnapshot> snapshot(long version) {
    return version == headVersion ? Optional.of(head) : Optional.empty();
  }

  @Override
  public MetadataHistory metadataVersions(long firstVersion, long lastVersion) {
    if (!metadataChanged) {
      return MetadataHistory.complete(List.of());
    }
    return MetadataHistory.gap(
        "Delta Sharing query history contains metadata without reliable version ordering");
  }

  @Override
  public UnorderedHistory unorderedHistory() {
    return unorderedHistory;
  }
}

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
import java.util.Optional;
import java.util.Set;

/** Versioned Delta schema and metadata history used by canonical identity reconciliation. */
public interface DeltaHistorySource {
  long MAX_IDENTITY_HISTORY_COMMITS = 100_000L;

  String tableName();

  long headVersion();

  HistoryTier historyTier();

  /** Whether versions returned by this source are confirmed Delta transaction-log versions. */
  default boolean sourceVersionAnchored() {
    return true;
  }

  Optional<Snapshot> snapshot(long version);

  /** Structural reason a historical snapshot cannot be loaded, if the source can prove one. */
  default Optional<String> snapshotExclusionReason(long version) {
    return Optional.empty();
  }

  MetadataHistory metadataVersions(long firstVersion, long lastVersion);

  /** Metadata and protocol observations whose relative ordering the source cannot prove. */
  default UnorderedHistory unorderedHistory() {
    return UnorderedHistory.EMPTY;
  }

  record HistoricalMetadata(String schemaJson, Map<String, String> configuration) {
    public HistoricalMetadata {
      schemaJson = schemaJson == null ? "" : schemaJson;
      configuration = configuration == null ? Map.of() : Map.copyOf(configuration);
    }
  }

  record HistoricalProtocol(
      int minReaderVersion,
      int minWriterVersion,
      Set<String> readerFeatures,
      Set<String> writerFeatures) {
    public HistoricalProtocol {
      readerFeatures = readerFeatures == null ? Set.of() : Set.copyOf(readerFeatures);
      writerFeatures = writerFeatures == null ? Set.of() : Set.copyOf(writerFeatures);
    }

    public boolean supportsColumnMapping() {
      return (minReaderVersion >= 2 && minWriterVersion >= 5)
          || readerFeatures.contains("columnMapping")
          || writerFeatures.contains("columnMapping");
    }
  }

  record UnorderedHistory(List<HistoricalMetadata> metadata, List<HistoricalProtocol> protocols) {
    private static final UnorderedHistory EMPTY = new UnorderedHistory(List.of(), List.of());

    public UnorderedHistory {
      metadata = metadata == null ? List.of() : List.copyOf(metadata);
      protocols = protocols == null ? List.of() : List.copyOf(protocols);
    }
  }

  record MetadataHistory(List<Long> versions, boolean complete, String reason) {
    public MetadataHistory {
      versions = List.copyOf(versions);
      reason = reason == null ? "" : reason;
    }

    public static MetadataHistory complete(List<Long> versions) {
      return new MetadataHistory(versions, true, "");
    }

    public static MetadataHistory gap(String reason) {
      return new MetadataHistory(List.of(), false, reason);
    }
  }
}

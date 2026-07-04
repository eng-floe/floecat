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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.metadata;

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asStringList;
import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asStringMap;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

@ApplicationScoped
public class TableCommitMetadataMutator {
  private static final Set<String> RESERVED_REMOVE_PROPERTIES = Set.of("format-version");

  @Inject TableCommitDefinitionSupport definitionSupport = new TableCommitDefinitionSupport();

  @Inject
  TableCommitSnapshotMetadataSupport snapshotMetadataSupport =
      new TableCommitSnapshotMetadataSupport();

  @Inject
  TableCommitMetadataNormalizationSupport normalizationSupport =
      new TableCommitMetadataNormalizationSupport();

  public TableMetadataView apply(TableMetadataView metadata, TableRequests.Commit req) {
    if (metadata == null || req == null || req.updates() == null || req.updates().isEmpty()) {
      return metadata;
    }
    CommitUpdateInspector.Parsed parsed = CommitUpdateInspector.inspect(req);
    TableMetadataView updated = applyPropertyUpdates(metadata, req);
    updated = definitionSupport.mergeTableDefinitionUpdates(updated, req);
    updated = preferSequenceAtLeast(updated, parsed.requestedSequenceNumber());
    updated = preferSequenceAtLeast(updated, parsed.maxSnapshotSequenceNumber());
    updated = snapshotMetadataSupport.mergeSnapshotUpdates(updated, parsed);
    return normalizationSupport.normalizeResponseMetadata(updated);
  }

  private TableMetadataView applyPropertyUpdates(
      TableMetadataView metadata, TableRequests.Commit req) {
    Map<String, String> props =
        metadata.properties() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(metadata.properties());
    boolean mutated = false;
    for (Map<String, Object> update : req.updates()) {
      CommitUpdateInspector.UpdateAction action = CommitUpdateInspector.actionTypeOf(update);
      if (action == null) {
        continue;
      }
      switch (action) {
        case SET_PROPERTIES -> {
          Map<String, String> updates = new LinkedHashMap<>(asStringMap(update.get("updates")));
          updates.remove("metadata-location");
          if (!updates.isEmpty()) {
            props.putAll(updates);
            mutated = true;
          }
        }
        case REMOVE_PROPERTIES -> {
          for (String removal : asStringList(update.get("removals"))) {
            if (RESERVED_REMOVE_PROPERTIES.contains(removal)) {
              continue;
            }
            mutated |= props.remove(removal) != null;
          }
        }
        default -> {
          // Ignore non-property actions.
        }
      }
    }
    if (!mutated) {
      return metadata;
    }
    return TableMetadataViewSupport.copyMetadata(metadata).properties(Map.copyOf(props)).build();
  }

  private TableMetadataView preferSequenceAtLeast(
      TableMetadataView metadata, Long candidateSequence) {
    if (candidateSequence == null || candidateSequence <= 0) {
      return metadata;
    }
    Long existing = metadata.lastSequenceNumber();
    long effective = existing == null ? candidateSequence : Math.max(existing, candidateSequence);
    if (existing != null && existing >= effective) {
      return metadata;
    }
    Map<String, String> props =
        metadata.properties() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(metadata.properties());
    props.put("last-sequence-number", Long.toString(effective));
    return TableMetadataViewSupport.copyMetadata(metadata)
        .properties(Map.copyOf(props))
        .lastSequenceNumber(effective)
        .build();
  }
}

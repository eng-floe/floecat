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

import ai.floedb.floecat.client.sharing.DeltaSharingModel.Protocol;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TableDescription;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TableHistory;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TableMetadata;
import ai.floedb.floecat.delta.identity.DeltaHistorySource;
import ai.floedb.floecat.delta.identity.DeltaIdentitySnapshot;
import ai.floedb.floecat.schema.identity.HistoryTier;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/** Ordered Delta schema history reconstructed from a Delta Sharing change-feed response. */
final class DeltaSharingHistorySource implements DeltaHistorySource {
  private final String tableName;
  private final long headVersion;
  private final NavigableMap<Long, DeltaIdentitySnapshot> snapshots;
  private final List<Long> metadataVersions;

  DeltaSharingHistorySource(
      String tableName,
      long baseVersion,
      TableDescription base,
      long headVersion,
      TableDescription head,
      TableHistory changes) {
    this.tableName = tableName;
    this.headVersion = headVersion;
    this.snapshots = new TreeMap<>();
    this.metadataVersions = build(baseVersion, base, headVersion, head, changes);
  }

  private List<Long> build(
      long baseVersion,
      TableDescription base,
      long headVersion,
      TableDescription head,
      TableHistory changes) {
    TableMetadata metadata = base.metadata();
    Protocol protocol = base.protocol();
    snapshots.put(
        baseVersion, DeltaSharingIdentityActions.snapshot(baseVersion, metadata, protocol));

    Map<Long, TableMetadata> metadataByVersion = new TreeMap<>();
    for (TableMetadata action : changes.metadata()) {
      long version =
          action
              .version()
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "Delta Sharing change-feed metadata has no version"));
      if (metadataByVersion.put(version, action) != null) {
        throw new IllegalArgumentException(
            "Delta Sharing change feed repeats metadata version " + version);
      }
    }
    Map<Long, Protocol> protocolByVersion = new TreeMap<>();
    for (ai.floedb.floecat.client.sharing.DeltaSharingModel.HistoricalProtocol action :
        changes.protocols()) {
      long version =
          action
              .version()
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "Delta Sharing change-feed protocol has no version"));
      if (protocolByVersion.put(version, action.protocol()) != null) {
        throw new IllegalArgumentException(
            "Delta Sharing change feed repeats protocol version " + version);
      }
    }
    Set<Long> eventVersions = new LinkedHashSet<>();
    eventVersions.addAll(metadataByVersion.keySet());
    eventVersions.addAll(protocolByVersion.keySet());
    List<Long> ordered = eventVersions.stream().sorted().toList();
    List<Long> retained = new ArrayList<>();
    for (long version : ordered) {
      if (version <= baseVersion || version > headVersion) {
        throw new IllegalArgumentException(
            "Delta Sharing change feed returned version "
                + version
                + " outside requested range "
                + (baseVersion + 1L)
                + ".."
                + headVersion);
      }
      metadata = metadataByVersion.getOrDefault(version, metadata);
      protocol = protocolByVersion.getOrDefault(version, protocol);
      snapshots.put(version, DeltaSharingIdentityActions.snapshot(version, metadata, protocol));
      retained.add(version);
    }
    snapshots.put(
        headVersion,
        DeltaSharingIdentityActions.snapshot(headVersion, head.metadata(), head.protocol()));
    return List.copyOf(retained);
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
    return HistoryTier.VERSIONED_CHANGE_FEED;
  }

  @Override
  public Optional<DeltaIdentitySnapshot> snapshot(long version) {
    return Optional.ofNullable(snapshots.get(version));
  }

  @Override
  public MetadataHistory metadataVersions(long firstVersion, long lastVersion) {
    if (lastVersion < firstVersion) {
      return MetadataHistory.complete(List.of());
    }
    if (lastVersion - firstVersion + 1L > MAX_IDENTITY_HISTORY_COMMITS) {
      return MetadataHistory.gap("Delta Sharing history exceeds the identity history limit");
    }
    if (snapshots.floorKey(firstVersion - 1L) == null && !snapshots.containsKey(firstVersion)) {
      return MetadataHistory.gap("Delta Sharing did not provide the predecessor schema");
    }
    return MetadataHistory.complete(
        metadataVersions.stream()
            .filter(version -> version >= firstVersion && version <= lastVersion)
            .toList());
  }
}

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

package ai.floedb.floecat.gateway.iceberg.rest.services.compat;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@ApplicationScoped
public class DeltaIcebergMetadataService {
  @Inject SnapshotClient snapshotClient;
  @Inject DeltaIcebergMetadataTranslator translator;
  @Inject DeltaManifestMaterializer manifestMaterializer;

  public DeltaLoadResult load(ResourceId tableId, Table table, SnapshotLister.Mode mode) {
    List<Snapshot> allSnapshots =
        SnapshotLister.fetchSnapshots(snapshotClient, tableId, SnapshotLister.Mode.ALL, null);
    IcebergMetadata metadata = translator.translate(table, allSnapshots);
    List<Snapshot> requestedSnapshots = snapshotsForMode(mode, allSnapshots, metadata);
    requestedSnapshots = manifestMaterializer.materialize(table, requestedSnapshots);
    return new DeltaLoadResult(metadata, requestedSnapshots);
  }

  private List<Snapshot> snapshotsForMode(
      SnapshotLister.Mode mode, List<Snapshot> snapshots, IcebergMetadata metadata) {
    if (mode != SnapshotLister.Mode.REFS) {
      return snapshots == null ? List.of() : snapshots;
    }
    if (metadata == null || metadata.getRefsCount() == 0) {
      return List.of();
    }
    Set<Long> refIds =
        metadata.getRefsMap().values().stream()
            .map(ref -> ref.getSnapshotId())
            .collect(Collectors.toSet());
    return snapshots == null
        ? List.of()
        : snapshots.stream().filter(s -> refIds.contains(s.getSnapshotId())).toList();
  }

  public record DeltaLoadResult(IcebergMetadata metadata, List<Snapshot> snapshots) {}
}

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

package ai.floedb.floecat.gateway.iceberg.minimal.services.compat;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@ApplicationScoped
public class DeltaIcebergMetadataService {
  @Inject MinimalGatewayConfig config;
  @Inject TableFormatSupport tableFormatSupport;
  @Inject DeltaIcebergMetadataTranslator translator;
  @Inject DeltaManifestMaterializer manifestMaterializer;

  public boolean enabledFor(Table table) {
    return config != null
        && config.deltaCompat().isPresent()
        && config.deltaCompat().get().enabled()
        && tableFormatSupport != null
        && tableFormatSupport.isDelta(table);
  }

  public DeltaLoadResult load(Table table, List<Snapshot> snapshots) {
    IcebergMetadata metadata = translator.translate(table, snapshots);
    List<Snapshot> requestedSnapshots = snapshotsForRefs(metadata, snapshots);
    requestedSnapshots = manifestMaterializer.materialize(table, requestedSnapshots, metadata);
    return new DeltaLoadResult(metadata, requestedSnapshots);
  }

  private List<Snapshot> snapshotsForRefs(IcebergMetadata metadata, List<Snapshot> snapshots) {
    if (metadata == null
        || metadata.getRefsCount() == 0
        || snapshots == null
        || snapshots.isEmpty()) {
      return snapshots == null ? List.of() : snapshots;
    }
    Set<Long> refIds =
        metadata.getRefsMap().values().stream()
            .map(ref -> ref.getSnapshotId())
            .collect(Collectors.toSet());
    List<Snapshot> filtered =
        snapshots.stream().filter(s -> refIds.contains(s.getSnapshotId())).toList();
    return filtered.isEmpty() ? snapshots : filtered;
  }

  public record DeltaLoadResult(IcebergMetadata metadata, List<Snapshot> snapshots) {}
}

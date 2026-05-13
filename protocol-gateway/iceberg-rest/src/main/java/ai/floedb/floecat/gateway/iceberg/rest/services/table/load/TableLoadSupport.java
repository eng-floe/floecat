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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.load;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.common.IcebergHttpUtil;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.DeltaManifestMaterializer;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.TableFormatSupport;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

@ApplicationScoped
public class TableLoadSupport {
  @Inject IcebergGatewayConfig config;
  @Inject GrpcServiceFacade snapshotClient;
  @Inject TableFormatSupport tableFormatSupport;
  @Inject DeltaManifestMaterializer deltaManifestMaterializer;

  LoadData loadData(
      Table tableRecord, SnapshotLister.Mode snapshotMode, TableGatewaySupport tableSupport) {
    String metadataLocation;
    List<Snapshot> snapshotList;
    if (deltaCompatEnabled(tableRecord)) {
      metadataLocation = null;
      snapshotList = SnapshotLister.fetchSnapshots(snapshotClient, tableRecord, snapshotMode);
      snapshotList = deltaManifestMaterializer.materialize(tableRecord, snapshotList);
    } else {
      metadataLocation = tableSupport.loadCurrentMetadataLocation(tableRecord);
      snapshotList = SnapshotLister.fetchSnapshots(snapshotClient, tableRecord, snapshotMode);
    }
    return new LoadData(metadataLocation, snapshotList);
  }

  SnapshotLister.Mode parseSnapshotMode(String raw) {
    if (raw == null || raw.isBlank() || raw.equalsIgnoreCase("all")) {
      return SnapshotLister.Mode.ALL;
    }
    if ("refs".equalsIgnoreCase(raw)) {
      return SnapshotLister.Mode.REFS;
    }
    throw new IllegalArgumentException("snapshots must be one of [all, refs]");
  }

  String etagValue(String metadataLocation, SnapshotLister.Mode snapshotMode) {
    String source = etagSource(metadataLocation, snapshotMode);
    return source == null ? null : IcebergHttpUtil.etagForMetadataLocation(source);
  }

  boolean hasWildcardIfNoneMatch(String ifNoneMatch) {
    return ifNoneMatch != null && ifNoneMatch.trim().equals("*");
  }

  boolean etagMatches(String etagValue, String ifNoneMatch) {
    if (etagValue == null || ifNoneMatch == null) {
      return false;
    }
    String expected = normalizeEtag(etagValue);
    for (String raw : ifNoneMatch.split(",")) {
      String token = normalizeEtag(raw);
      if (!token.isEmpty() && token.equals(expected)) {
        return true;
      }
    }
    return false;
  }

  private String normalizeEtag(String token) {
    if (token == null) {
      return "";
    }
    String value = token.trim();
    if (value.startsWith("W/")) {
      value = value.substring(2).trim();
    }
    if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
      value = value.substring(1, value.length() - 1);
    }
    return value;
  }

  private String etagSource(String metadataLocation, SnapshotLister.Mode snapshotMode) {
    if (metadataLocation == null) {
      return null;
    }
    String mode =
        snapshotMode == null
            ? SnapshotLister.Mode.ALL.name().toLowerCase()
            : snapshotMode.name().toLowerCase();
    return metadataLocation + "|snapshots=" + mode;
  }

  private boolean deltaCompatEnabled(Table table) {
    if (config == null || tableFormatSupport == null || table == null) {
      return false;
    }
    var deltaCompat = config.deltaCompat();
    return deltaCompat.isPresent()
        && deltaCompat.get().enabled()
        && tableFormatSupport.isDelta(table);
  }

  record LoadData(String metadataLocation, List<Snapshot> snapshots) {}
}

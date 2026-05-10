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

package ai.floedb.floecat.service.common;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public final class IcebergMetadataLocationResolver {
  private static final String METADATA_LOCATION_KEY = "metadata-location";
  private static final String ICEBERG_FORMAT_METADATA_KEY = "iceberg";

  private IcebergMetadataLocationResolver() {}

  public static String resolve(Table table, Snapshot snapshot) {
    String tableLocation = tableMetadataLocation(table);
    if (tableLocation != null) {
      return tableLocation;
    }
    return snapshotMetadataLocation(snapshot);
  }

  private static String tableMetadataLocation(Table table) {
    if (table == null || table.getPropertiesCount() == 0) {
      return null;
    }
    String value = table.getPropertiesMap().get(METADATA_LOCATION_KEY);
    if (value == null) {
      return null;
    }
    value = value.trim();
    return value.isBlank() ? null : value;
  }

  private static String snapshotMetadataLocation(Snapshot snapshot) {
    if (snapshot == null) {
      return null;
    }
    ByteString raw =
        snapshot.getFormatMetadataOrDefault(ICEBERG_FORMAT_METADATA_KEY, ByteString.EMPTY);
    if (raw == null || raw.isEmpty()) {
      return null;
    }
    try {
      IcebergMetadata metadata = IcebergMetadata.parseFrom(raw);
      String value = metadata.getMetadataLocation();
      if (value == null) {
        return null;
      }
      value = value.trim();
      return value.isBlank() ? null : value;
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          "Failed to parse Iceberg metadata for snapshot " + snapshot.getSnapshotId(), e);
    }
  }
}

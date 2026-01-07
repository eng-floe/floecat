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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public final class SnapshotMetadataUtil {
  private static final String ICEBERG_METADATA_KEY = "iceberg";

  private SnapshotMetadataUtil() {}

  public static IcebergMetadata parseSnapshotMetadata(Snapshot snapshot) {
    if (snapshot == null) {
      return null;
    }
    ByteString raw = snapshot.getFormatMetadataOrDefault(ICEBERG_METADATA_KEY, ByteString.EMPTY);
    if (raw == null || raw.isEmpty()) {
      return null;
    }
    try {
      return IcebergMetadata.parseFrom(raw);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          "Failed to parse Iceberg metadata for snapshot " + snapshot.getSnapshotId(), e);
    }
  }
}

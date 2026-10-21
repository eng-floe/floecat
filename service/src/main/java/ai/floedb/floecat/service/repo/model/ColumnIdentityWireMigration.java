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

package ai.floedb.floecat.service.repo.model;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import java.nio.ByteBuffer;

/** Removes retired inline identity payloads while decoding persisted catalog objects. */
public final class ColumnIdentityWireMigration {
  private static final int SNAPSHOT_INLINE_IDENTITY_FIELD = 14;
  private static final int TABLE_INLINE_IDENTITY_FIELD = 9;

  private ColumnIdentityWireMigration() {}

  public static Snapshot parseSnapshot(byte[] bytes) throws InvalidProtocolBufferException {
    return stripSnapshot(Snapshot.parseFrom(bytes));
  }

  public static Snapshot parseSnapshot(ByteBuffer bytes) throws InvalidProtocolBufferException {
    return stripSnapshot(Snapshot.parseFrom(bytes));
  }

  public static Table parseTable(byte[] bytes) throws InvalidProtocolBufferException {
    return stripTable(Table.parseFrom(bytes));
  }

  public static Table parseTable(ByteBuffer bytes) throws InvalidProtocolBufferException {
    return stripTable(Table.parseFrom(bytes));
  }

  private static Snapshot stripSnapshot(Snapshot value) {
    UnknownFieldSet original = value.getUnknownFields();
    UnknownFieldSet unknown = withoutField(original, SNAPSHOT_INLINE_IDENTITY_FIELD);
    return unknown == original ? value : value.toBuilder().setUnknownFields(unknown).build();
  }

  private static Table stripTable(Table value) {
    UnknownFieldSet original = value.getUnknownFields();
    UnknownFieldSet unknown = withoutField(original, TABLE_INLINE_IDENTITY_FIELD);
    return unknown == original ? value : value.toBuilder().setUnknownFields(unknown).build();
  }

  private static UnknownFieldSet withoutField(UnknownFieldSet unknown, int fieldNumber) {
    if (!unknown.hasField(fieldNumber)) {
      return unknown;
    }
    return unknown.toBuilder().clearField(fieldNumber).build();
  }
}

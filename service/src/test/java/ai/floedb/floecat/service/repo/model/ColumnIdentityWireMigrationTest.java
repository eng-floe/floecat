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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.CodedOutputStream;
import java.io.ByteArrayOutputStream;
import org.junit.jupiter.api.Test;

class ColumnIdentityWireMigrationTest {
  private static final byte[] LEGACY_INLINE_IDENTITY = {0x08, 0x01};

  @Test
  void stripsOnlyTheRetiredSnapshotIdentityPayload() throws Exception {
    var bytes = new ByteArrayOutputStream();
    var output = CodedOutputStream.newInstance(bytes);
    output.writeInt64(2, 7L);
    output.writeByteArray(14, LEGACY_INLINE_IDENTITY);
    output.writeUInt32(50, 3);
    output.flush();

    var snapshot = ColumnIdentityWireMigration.parseSnapshot(bytes.toByteArray());

    assertEquals(7L, snapshot.getSnapshotId());
    assertFalse(snapshot.hasColumnIdentityMapRef());
    assertFalse(snapshot.getUnknownFields().hasField(14));
    assertTrue(snapshot.getUnknownFields().hasField(50));
  }

  @Test
  void stripsTheRetiredTableIdentityPayload() throws Exception {
    var bytes = new ByteArrayOutputStream();
    var output = CodedOutputStream.newInstance(bytes);
    output.writeByteArray(9, LEGACY_INLINE_IDENTITY);
    output.flush();

    var table = ColumnIdentityWireMigration.parseTable(bytes.toByteArray());

    assertFalse(table.hasColumnIdentityMapRef());
    assertFalse(table.getUnknownFields().hasField(9));
  }
}

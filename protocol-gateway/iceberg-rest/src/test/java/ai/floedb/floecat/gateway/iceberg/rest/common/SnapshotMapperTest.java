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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import java.util.List;
import org.junit.jupiter.api.Test;

class SnapshotMapperTest {

  @Test
  void snapshotsOmitUnsetParentSnapshotIdButPreserveExplicitZero() {
    Snapshot withoutParent = Snapshot.newBuilder().setSnapshotId(10L).build();
    Snapshot explicitZeroParent =
        Snapshot.newBuilder().setSnapshotId(11L).setParentSnapshotId(0L).build();

    List<java.util.Map<String, Object>> mapped =
        SnapshotMapper.snapshots(List.of(withoutParent, explicitZeroParent));

    assertFalse(mapped.get(0).containsKey("parent-snapshot-id"));
    assertTrue(mapped.get(1).containsKey("parent-snapshot-id"));
    assertEquals(0L, mapped.get(1).get("parent-snapshot-id"));
  }
}

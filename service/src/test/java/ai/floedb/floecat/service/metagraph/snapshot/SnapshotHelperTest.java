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

package ai.floedb.floecat.service.metagraph.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.testsupport.SnapshotTestSupport;
import ai.floedb.floecat.service.testsupport.TestNodes;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnapshotHelperTest {

  private SnapshotHelper helper;
  private SnapshotTestSupport.FakeSnapshotRepository repository;
  private SnapshotTestSupport.FakeSnapshotClient snapshotClient;

  @BeforeEach
  void setUp() {
    repository = new SnapshotTestSupport.FakeSnapshotRepository();
    helper = new SnapshotHelper(repository, null);
    snapshotClient = new SnapshotTestSupport.FakeSnapshotClient();
    helper.setSnapshotClient(snapshotClient);
  }

  @Test
  void pinUsesExplicitSnapshotId() {
    SnapshotPin pin =
        helper.snapshotPinFor(
            "corr",
            tableId("tbl"),
            SnapshotRef.newBuilder().setSnapshotId(5).build(),
            Optional.empty());

    assertThat(pin.getSnapshotId()).isEqualTo(5);
  }

  @Test
  void pinUsesAsOfOverride() {
    Timestamp ts = ts("2024-01-01T00:00:00Z");
    SnapshotPin pin =
        helper.snapshotPinFor(
            "corr", tableId("tbl"), SnapshotRef.newBuilder().setAsOf(ts).build(), Optional.empty());

    assertThat(pin.getAsOf()).isEqualTo(ts);
    assertThat(pin.getSnapshotId()).isZero();
  }

  @Test
  void pinUsesAsOfDefault() {
    Timestamp ts = ts("2024-02-01T00:00:00Z");
    SnapshotPin pin = helper.snapshotPinFor("corr", tableId("tbl"), null, Optional.of(ts));

    assertThat(pin.getAsOf()).isEqualTo(ts);
  }

  @Test
  void pinFallsBackToSnapshotService() {
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(99L)
            .setUpstreamCreatedAt(ts("2024-03-01T00:00:00Z"))
            .build();
    snapshotClient.nextResponse = GetSnapshotResponse.newBuilder().setSnapshot(snapshot).build();

    SnapshotPin pin = helper.snapshotPinFor("corr", tableId("tbl"), null, Optional.empty());

    assertThat(pin.getSnapshotId()).isEqualTo(99L);
    assertThat(snapshotClient.lastRequest.getSnapshot().getSpecial())
        .isEqualTo(SpecialSnapshot.SS_CURRENT);
  }

  @Test
  void schemaJsonUsesSnapshotPayload() {
    ResourceId tableId = tableId("tbl");
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(41L)
            .setSchemaJson("{\"fields\":[]}")
            .setUpstreamCreatedAt(ts("2024-04-01T00:00:00Z"))
            .build();
    repository.put(tableId, snapshot);

    String schema =
        helper.schemaJsonFor(
            "corr",
            TestNodes.tableNode(tableId, "{}"),
            SnapshotRef.newBuilder().setSnapshotId(41L).build(),
            () -> "{}");

    assertThat(schema).contains("fields");
  }

  @Test
  void schemaJsonThrowsWhenSnapshotMissing() {
    ResourceId tableId = tableId("tbl");
    assertThatThrownBy(
            () ->
                helper.schemaJsonFor(
                    "corr",
                    TestNodes.tableNode(tableId, "{}"),
                    SnapshotRef.newBuilder().setSnapshotId(1L).build(),
                    () -> "{}"))
        .isInstanceOf(RuntimeException.class);
  }

  private static ResourceId tableId(String id) {
    return ResourceId.newBuilder()
        .setAccountId("account")
        .setId(id)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }

  private static Timestamp ts(String instant) {
    Instant parsed = Instant.parse(instant);
    return Timestamp.newBuilder()
        .setSeconds(parsed.getEpochSecond())
        .setNanos(parsed.getNano())
        .build();
  }
}

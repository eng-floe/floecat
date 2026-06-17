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

package ai.floedb.floecat.service.reconciler.jobs.durable.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

class StoredFileGroupResultPayloadTest {
  @Test
  void jsonRoundTripPreservesPartialAggregateRecords() throws Exception {
    TargetStatsRecord partial =
        TargetStatsRecords.tableRecord(
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_TABLE)
                .setId("table-1")
                .build(),
            55L,
            TableValueStats.newBuilder().setRowCount(7L).build(),
            null);
    ReconcileFileGroupTask task =
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            1,
            "/blob/file-stats.json",
            3,
            List.of("s3://bucket/file-1.parquet"),
            List.of(),
            List.of(partial));

    ObjectMapper mapper = new ObjectMapper();
    StoredFileGroupResultPayload decoded =
        mapper.readValue(
            mapper.writeValueAsBytes(StoredFileGroupResultPayload.of(task)),
            StoredFileGroupResultPayload.class);

    assertEquals(task.fileStatsBlobUri(), decoded.fileStatsBlobUri());
    assertEquals(task.fileStatsRecordCount(), decoded.fileStatsRecordCount());
    assertEquals(task.filePaths(), decoded.filePaths());
    assertEquals(task.partialAggregateRecords(), decoded.partialAggregateRecords());
  }
}

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

package ai.floedb.floecat.gateway.iceberg.rest.services.planning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.ContentFileDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.FileScanTaskDto;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PlanTaskManagerTest {
  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_TABLE)
          .setId("tbl-1")
          .build();
  private static final ResourceId OTHER_TABLE_ID = TABLE_ID.toBuilder().setId("tbl-2").build();

  private final IcebergGatewayConfig config = mock(IcebergGatewayConfig.class);
  private PlanTaskManager manager;

  @BeforeEach
  void setUp() {
    when(config.planTaskTtl()).thenReturn(Duration.ofMinutes(5));
    when(config.planTaskFilesPerTask()).thenReturn(10);
    manager = new PlanTaskManager(config);
  }

  @Test
  void registerCompletedPlanRetainsFilesAndDeletes() {
    ContentFileDto dataFile =
        new ContentFileDto(
            "data",
            "s3://bucket/data.parquet",
            "PARQUET",
            1,
            List.of(),
            1L,
            1L,
            null,
            List.of(),
            null,
            null);
    ContentFileDto deleteFile =
        new ContentFileDto(
            "position-deletes",
            "s3://bucket/delete.parquet",
            "PARQUET",
            1,
            List.of(),
            1L,
            1L,
            null,
            List.of(),
            null,
            null);
    List<FileScanTaskDto> tasks = List.of(new FileScanTaskDto(dataFile, List.of(), null));
    List<ContentFileDto> deletes = List.of(deleteFile);

    PlanTaskManager.PlanDescriptor descriptor =
        manager.registerCompletedPlan("plan-1", TABLE_ID, "ns", "tbl", tasks, deletes, List.of());

    assertEquals(tasks, descriptor.fileScanTasks());
    assertEquals(deletes, descriptor.deleteFiles());

    PlanTaskManager.PlanDescriptor fetched =
        manager.findPlan("plan-1", TABLE_ID).orElseThrow(() -> new AssertionError("plan missing"));
    assertEquals(tasks, fetched.fileScanTasks());
    assertEquals(deletes, fetched.deleteFiles());
  }

  @Test
  void planLookupRequiresMatchingTableIdentity() {
    manager.registerSubmittedPlan("plan-1", TABLE_ID, "ns", "tbl");

    assertTrue(manager.findPlan("plan-1", OTHER_TABLE_ID).isEmpty());
  }

  @Test
  void cancelPlanRequiresMatchingTableIdentity() {
    manager.registerSubmittedPlan("plan-1", TABLE_ID, "ns", "tbl");

    manager.cancelPlan("plan-1", OTHER_TABLE_ID);

    assertTrue(manager.findPlan("plan-1", TABLE_ID).isPresent());
    manager.cancelPlan("plan-1", TABLE_ID);
    assertEquals(
        PlanTaskManager.PlanStatus.CANCELLED,
        manager.findPlan("plan-1", TABLE_ID).orElseThrow().status());
  }
}

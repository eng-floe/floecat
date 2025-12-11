package ai.floedb.floecat.gateway.iceberg.rest.services.planning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.ContentFileDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.FileScanTaskDto;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PlanTaskManagerTest {

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
        manager.registerCompletedPlan("plan-1", "ns", "tbl", tasks, deletes, List.of());

    assertEquals(tasks, descriptor.fileScanTasks());
    assertEquals(deletes, descriptor.deleteFiles());

    PlanTaskManager.PlanDescriptor fetched =
        manager.findPlan("plan-1").orElseThrow(() -> new AssertionError("plan missing"));
    assertEquals(tasks, fetched.fileScanTasks());
    assertEquals(deletes, fetched.deleteFiles());
  }
}

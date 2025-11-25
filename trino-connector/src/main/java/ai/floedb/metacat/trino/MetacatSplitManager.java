package ai.floedb.metacat.trino;

import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.planning.rpc.BeginPlanExRequest;
import ai.floedb.metacat.planning.rpc.PlanInput;
import ai.floedb.metacat.planning.rpc.PlanningExGrpc;
import com.google.inject.Inject;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergSplit;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.TupleDomain;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;

public class MetacatSplitManager implements ConnectorSplitManager {

  private final PlanningExGrpc.PlanningExBlockingStub planning;

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(MetacatSplitManager.class);

  @Inject
  public MetacatSplitManager(PlanningExGrpc.PlanningExBlockingStub planning) {
    this.planning = planning;
  }

  @Override
  public ConnectorSplitSource getSplits(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorTableHandle handle,
      DynamicFilter dynamicFilter,
      Constraint constraint) {
    MetacatTableHandle metacatHandle = (MetacatTableHandle) handle;

    ResourceId rid = metacatHandle.getTableResourceId();
    LOG.debug(
        "beginPlanEx for tableId id={}, tenant={}, kind={}",
        rid.getId(),
        rid.getTenantId(),
        rid.getKind());

    BeginPlanExRequest request =
        BeginPlanExRequest.newBuilder()
            .addInputs(
                PlanInput.newBuilder().setTableId(metacatHandle.getTableResourceId()).build())
            .setIncludeSchema(false)
            .build();
    var response = planning.beginPlanEx(request);

    String partitionSpecJson =
        Optional.ofNullable(metacatHandle.getPartitionSpecJson())
            .orElse(PartitionSpecParser.toJson(PartitionSpec.unpartitioned()));
    // Iceberg expects {"partition_values":[]} for unpartitioned tables.
    String partitionDataJson = "{\"partition_values\":[]}";

    List<IcebergSplit> splits = new ArrayList<>();
    for (var file : response.getPlan().getDataFilesList()) {
      IcebergFileFormat fileFormat = toIcebergFormat(file.getFileFormat());
      String dataJson = partitionDataJson;
      IcebergSplit split =
          new IcebergSplit(
              file.getFilePath(),
              0,
              file.getFileSizeInBytes(),
              file.getFileSizeInBytes(),
              file.getRecordCount(),
              fileFormat,
              java.util.Optional.of(java.util.List.of()),
              partitionSpecJson,
              dataJson,
              List.of(), // TODO: hook delete files
              io.trino.spi.SplitWeight.standard(),
              TupleDomain.all(),
              Map.of(), // TODO: pass file IO properties (S3 credentials)
              List.of(),
              0);
      splits.add(split);
    }

    return new FixedSplitSource(splits);
  }

  private static IcebergFileFormat toIcebergFormat(String format) {
    if (format == null || format.isBlank()) {
      return IcebergFileFormat.PARQUET;
    }
    String upper = format.toUpperCase();
    // Metacat uses TF_* enums; default to PARQUET for TF_ICEBERG or unknowns.
    if (upper.startsWith("TF_")) {
      return IcebergFileFormat.PARQUET;
    }
    try {
      return IcebergFileFormat.valueOf(upper);
    } catch (IllegalArgumentException e) {
      return IcebergFileFormat.PARQUET;
    }
  }
}

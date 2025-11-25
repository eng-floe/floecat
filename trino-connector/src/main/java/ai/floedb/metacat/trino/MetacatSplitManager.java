package ai.floedb.metacat.trino;

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

public class MetacatSplitManager implements ConnectorSplitManager {

  private final PlanningExGrpc.PlanningExBlockingStub planning;

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

    BeginPlanExRequest request =
        BeginPlanExRequest.newBuilder()
            .addInputs(PlanInput.newBuilder().setTableId(metacatHandle.getTableId()).build())
            .setIncludeSchema(false)
            .build();
    var response = planning.beginPlanEx(request);

    List<IcebergSplit> splits = new ArrayList<>();
    for (var file : response.getPlan().getDataFilesList()) {
      IcebergFileFormat fileFormat = IcebergFileFormat.valueOf(file.getFileFormat().toUpperCase());
      IcebergSplit split =
          new IcebergSplit(
              file.getFilePath(),
              0,
              file.getFileSizeInBytes(),
              file.getFileSizeInBytes(),
              file.getRecordCount(),
              fileFormat,
              java.util.Optional.empty(),
              metacatHandle.getPartitionSpecJson(),
              null,
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
}

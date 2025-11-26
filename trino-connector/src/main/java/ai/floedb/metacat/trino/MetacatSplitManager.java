package ai.floedb.metacat.trino;

import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.query.rpc.BeginQueryRequest;
import ai.floedb.metacat.query.rpc.Operator;
import ai.floedb.metacat.query.rpc.Predicate;
import ai.floedb.metacat.query.rpc.QueryInput;
import ai.floedb.metacat.query.rpc.QueryServiceGrpc;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.plugin.iceberg.IcebergColumnHandle;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;

public class MetacatSplitManager implements ConnectorSplitManager {

  private final QueryServiceGrpc.QueryServiceBlockingStub planning;

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(MetacatSplitManager.class);

  @Inject
  public MetacatSplitManager(QueryServiceGrpc.QueryServiceBlockingStub planning) {
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
        "beginQuery for tableId id={}, tenant={}, kind={}",
        rid.getId(),
        rid.getTenantId(),
        rid.getKind());

    TupleDomain<IcebergColumnHandle> staticDomain = metacatHandle.getEnforcedConstraint();

    TupleDomain<IcebergColumnHandle> dynamicDomain =
        constraint.getSummary().transformKeys(ch -> (IcebergColumnHandle) ch);

    TupleDomain<IcebergColumnHandle> effectiveDomain = staticDomain.intersect(dynamicDomain);

    Set<String> requiredColumns = new LinkedHashSet<>();
    List<Predicate> predicates = new ArrayList<>();

    effectiveDomain
        .getDomains()
        .ifPresent(
            domains -> {
              domains.forEach(
                  (colHandle, domain) -> {
                    String name = colHandle.getName();
                    requiredColumns.add(name);

                    if (domain.isNullAllowed() && domain.getValues().isAll()) {
                      predicates.add(
                          Predicate.newBuilder()
                              .setColumn(name)
                              .setOp(Operator.OP_IS_NULL)
                              .build());
                      return;
                    }

                    if (domain.isSingleValue()) {
                      Object v = domain.getSingleValue();
                      predicates.add(
                          Predicate.newBuilder()
                              .setColumn(name)
                              .setOp(Operator.OP_EQ)
                              .addValues(domainValueToString(v))
                              .build());
                      return;
                    }

                    if (domain.getValues().isDiscreteSet()) {
                      var builder = Predicate.newBuilder().setColumn(name).setOp(Operator.OP_IN);

                      domain
                          .getValues()
                          .getDiscreteValues()
                          .getValues()
                          .forEach(v -> builder.addValues(domainValueToString(v)));

                      predicates.add(builder.build());
                      return;
                    }

                    var ranges = domain.getValues().getRanges();
                    if (ranges.getRangeCount() == 1) {
                      var r = ranges.getOrderedRanges().get(0);

                      if (r.isSingleValue()) {
                        Object v = r.getSingleValue();
                        predicates.add(
                            Predicate.newBuilder()
                                .setColumn(name)
                                .setOp(Operator.OP_EQ)
                                .addValues(domainValueToString(v))
                                .build());
                        return;
                      }

                      boolean lowBounded = !r.isLowUnbounded();
                      boolean highBounded = !r.isHighUnbounded();

                      if (lowBounded && highBounded) {
                        Object low = r.getLowBoundedValue();
                        Object high = r.getHighBoundedValue();
                        predicates.add(
                            Predicate.newBuilder()
                                .setColumn(name)
                                .setOp(Operator.OP_BETWEEN)
                                .addValues(domainValueToString(low))
                                .addValues(domainValueToString(high))
                                .build());
                      } else if (lowBounded) {
                        Object low = r.getLowBoundedValue();
                        predicates.add(
                            Predicate.newBuilder()
                                .setColumn(name)
                                .setOp(r.isLowInclusive() ? Operator.OP_GTE : Operator.OP_GT)
                                .addValues(domainValueToString(low))
                                .build());
                      } else if (highBounded) {
                        Object high = r.getHighBoundedValue();
                        predicates.add(
                            Predicate.newBuilder()
                                .setColumn(name)
                                .setOp(r.isHighInclusive() ? Operator.OP_LTE : Operator.OP_LT)
                                .addValues(domainValueToString(high))
                                .build());
                      }
                      return;
                    }
                  });
            });

    LOG.debug(
        "split request: tableId={} requiredCols={} predicates={} staticDomain={} dynamicSummary={}",
        metacatHandle.getTableResourceId().getId(),
        requiredColumns,
        predicates,
        staticDomain,
        constraint.getSummary());

    BeginQueryRequest.Builder request =
        BeginQueryRequest.newBuilder()
            .addInputs(
                QueryInput.newBuilder().setTableId(metacatHandle.getTableResourceId()).build())
            .setIncludeSchema(false);
    if (!requiredColumns.isEmpty()) {
      request.addAllRequiredColumns(requiredColumns);
    }
    if (!predicates.isEmpty()) {
      request.addAllPredicates(predicates);
    }

    LOG.debug(
        "split request: tableId={} requiredCols={} predicates={} constraintSummary={}",
        metacatHandle.getTableResourceId().getId(),
        requiredColumns,
        predicates,
        constraint.getSummary());

    var response = planning.beginQuery(request.build());

    LOG.debug(
        "split response: dataFiles={} deleteFiles={}",
        response.getQuery().getDataFilesCount(),
        response.getQuery().getDeleteFilesCount());

    String partitionSpecJson =
        Optional.ofNullable(metacatHandle.getPartitionSpecJson())
            .orElse(PartitionSpecParser.toJson(PartitionSpec.unpartitioned()));
    String defaultPartitionDataJson = "{\"partitionValues\":[]}";

    List<IcebergSplit> splits = new ArrayList<>();
    for (var file : response.getQuery().getDataFilesList()) {
      IcebergFileFormat fileFormat = toIcebergFormat(file.getFileFormat());
      String dataJson = file.getPartitionDataJson();
      if (dataJson == null || dataJson.isBlank()) {
        dataJson = defaultPartitionDataJson;
      }
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
    if (upper.startsWith("TF_")) {
      return IcebergFileFormat.PARQUET;
    }
    try {
      return IcebergFileFormat.valueOf(upper);
    } catch (IllegalArgumentException e) {
      return IcebergFileFormat.PARQUET;
    }
  }

  private static String domainValueToString(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof Slice slice) {
      return slice.toStringUtf8();
    }
    return value.toString();
  }
}

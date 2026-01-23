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

package ai.floedb.floecat.client.trino;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.FetchScanBundleRequest;
import ai.floedb.floecat.common.rpc.Operator;
import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import com.google.inject.Inject;
import com.google.protobuf.Timestamp;
import io.airlift.slice.Slice;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergSplit;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.spi.SplitWeight;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;

public class FloecatSplitManager implements ConnectorSplitManager {

  private final QueryServiceGrpc.QueryServiceBlockingStub planning;
  private final QueryScanServiceGrpc.QueryScanServiceBlockingStub scan;
  private final QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub schema;
  private final DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;
  private final CatalogName catalogName;
  private final FloecatConfig config;
  private volatile ResourceId catalogResourceId;

  @Inject
  public FloecatSplitManager(
      QueryServiceGrpc.QueryServiceBlockingStub planning,
      QueryScanServiceGrpc.QueryScanServiceBlockingStub scan,
      QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub schema,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      CatalogName catalogName,
      FloecatConfig config) {
    this.planning = planning;
    this.scan = scan;
    this.schema = schema;
    this.directory = directory;
    this.catalogName = catalogName;
    this.config = config;
  }

  private ResourceId getCatalogResourceId() {
    if (catalogResourceId == null) {
      synchronized (this) {
        if (catalogResourceId == null) {
          var response = directory.resolveCatalog(
              ResolveCatalogRequest.newBuilder()
                  .setRef(NameRef.newBuilder().setCatalog(catalogName.toString()).build())
                  .build());
          catalogResourceId = response.getResourceId();
        }
      }
    }
    return catalogResourceId;
  }

  @Override
  public ConnectorSplitSource getSplits(
      ConnectorTransactionHandle transaction,
      ConnectorSession session,
      ConnectorTableHandle handle,
      DynamicFilter dynamicFilter,
      Constraint constraint) {

    FloecatTableHandle floecatHandle = (FloecatTableHandle) handle;

    TupleDomain<IcebergColumnHandle> staticDomain = floecatHandle.getEnforcedConstraint();
    TupleDomain<IcebergColumnHandle> constraintDomain =
        constraint.getSummary().transformKeys(ch -> (IcebergColumnHandle) ch);
    TupleDomain<IcebergColumnHandle> dynamicDomain =
        dynamicFilter.getCurrentPredicate().transformKeys(ch -> (IcebergColumnHandle) ch);

    TupleDomain<IcebergColumnHandle> effectiveDomain =
        staticDomain.intersect(constraintDomain).intersect(dynamicDomain);

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
                    predicates.addAll(domainToPredicates(name, domain));
                  });
            });
    requiredColumns.addAll(floecatHandle.getProjectedColumns());

    BeginQueryRequest beginReq = BeginQueryRequest.newBuilder()
        .setDefaultCatalogId(getCatalogResourceId())
        .build();

    var beginResp = planning.beginQuery(beginReq);
    String queryId = beginResp.getQuery().getQueryId();

    var describeReq =
        DescribeInputsRequest.newBuilder()
            .setQueryId(queryId)
            .addInputs(toQueryInput(floecatHandle.getTableResourceId(), floecatHandle))
            .build();

    schema.describeInputs(describeReq);

    FetchScanBundleRequest fetchReq =
        FetchScanBundleRequest.newBuilder()
            .setQueryId(queryId)
            .setTableId(floecatHandle.getTableResourceId())
            .addAllRequiredColumns(requiredColumns)
            .addAllPredicates(predicates)
            .build();

    var fetchResp = scan.fetchScanBundle(fetchReq);

    List<ScanFile> dataFiles = fetchResp.getBundle().getDataFilesList();
    List<ScanFile> deleteScanFiles = fetchResp.getBundle().getDeleteFilesList();
    List<DeleteFile> deleteFiles = toDeleteFiles(deleteScanFiles);

    String partitionSpecJson =
        Optional.ofNullable(floecatHandle.getPartitionSpecJson())
            .orElse(PartitionSpecParser.toJson(PartitionSpec.unpartitioned()));
    String defaultPartitionDataJson = "{\"partitionValues\":[]}";
    Map<String, String> storageProps = buildStorageProperties();

    List<IcebergSplit> splits = new ArrayList<>();
    for (var file : dataFiles) {
      List<DeleteFile> fileDeletes = selectDeleteFiles(deleteFiles, file.getDeleteFileIndicesList());
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
              fileDeletes,
              SplitWeight.standard(),
              TupleDomain.all(),
              storageProps,
              List.of(),
              file.getPartitionSpecId());
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

  private static QueryInput toQueryInput(ResourceId rid, FloecatTableHandle handle) {
    QueryInput.Builder b = QueryInput.newBuilder().setTableId(rid);
    if (handle.getSnapshotId() != null) {
      b.setSnapshot(SnapshotRef.newBuilder().setSnapshotId(handle.getSnapshotId()));
    } else if (handle.getAsOfEpochMillis() != null) {
      b.setSnapshot(SnapshotRef.newBuilder().setAsOf(toTimestamp(handle.getAsOfEpochMillis())));
    }
    return b.build();
  }

  private static Timestamp toTimestamp(long millis) {
    long seconds = Math.floorDiv(millis, 1000);
    int nanos = (int) ((millis % 1000) * 1_000_000);
    return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
  }

  private static List<Predicate> domainToPredicates(String column, Domain domain) {
    List<Predicate> out = new ArrayList<>();

    if (domain.isAll()) {
      return out;
    }

    if (!domain.isNullAllowed()) {
      out.add(Predicate.newBuilder().setColumn(column).setOp(Operator.OP_IS_NOT_NULL).build());
    }

    if (domain.isSingleValue()) {
      out.add(
          Predicate.newBuilder()
              .setColumn(column)
              .setOp(Operator.OP_EQ)
              .addValues(domainValueToString(domain.getSingleValue()))
              .build());
      return out;
    }

    if (domain.getValues().isDiscreteSet()) {
      var builder = Predicate.newBuilder().setColumn(column).setOp(Operator.OP_IN);
      domain
          .getValues()
          .getDiscreteValues()
          .getValues()
          .forEach(v -> builder.addValues(domainValueToString(v)));
      out.add(builder.build());
      return out;
    }

    var ranges = domain.getValues().getRanges();
    if (ranges.getRangeCount() == 1) {
      var r = ranges.getOrderedRanges().get(0);

      if (r.isSingleValue()) {
        out.add(
            Predicate.newBuilder()
                .setColumn(column)
                .setOp(Operator.OP_EQ)
                .addValues(domainValueToString(r.getSingleValue()))
                .build());
        return out;
      }

      boolean lowBounded = !r.isLowUnbounded();
      boolean highBounded = !r.isHighUnbounded();

      if (lowBounded && highBounded) {
        out.add(
            Predicate.newBuilder()
                .setColumn(column)
                .setOp(Operator.OP_BETWEEN)
                .addValues(domainValueToString(r.getLowBoundedValue()))
                .addValues(domainValueToString(r.getHighBoundedValue()))
                .build());
      } else if (lowBounded) {
        out.add(
            Predicate.newBuilder()
                .setColumn(column)
                .setOp(r.isLowInclusive() ? Operator.OP_GTE : Operator.OP_GT)
                .addValues(domainValueToString(r.getLowBoundedValue()))
                .build());
      } else if (highBounded) {
        out.add(
            Predicate.newBuilder()
                .setColumn(column)
                .setOp(r.isHighInclusive() ? Operator.OP_LTE : Operator.OP_LT)
                .addValues(domainValueToString(r.getHighBoundedValue()))
                .build());
      }
      return out;
    }

    boolean allSingleValues = ranges.getOrderedRanges().stream().allMatch(Range::isSingleValue);
    if (allSingleValues) {
      var builder = Predicate.newBuilder().setColumn(column).setOp(Operator.OP_IN);
      ranges
          .getOrderedRanges()
          .forEach(r -> builder.addValues(domainValueToString(r.getSingleValue())));
      out.add(builder.build());
    }

    return out;
  }

  private static List<DeleteFile> toDeleteFiles(List<ScanFile> files) {
    if (files == null || files.isEmpty()) {
      return List.of();
    }
    List<DeleteFile> out = new ArrayList<>(files.size());
    for (ScanFile file : files) {
      FileContent content =
          switch (file.getFileContent()) {
            case SCAN_FILE_CONTENT_EQUALITY_DELETES -> FileContent.EQUALITY_DELETES;
            case SCAN_FILE_CONTENT_POSITION_DELETES -> FileContent.POSITION_DELETES;
            default -> null;
          };
      if (content == null) {
        continue;
      }
      FileFormat format;
      try {
        format = FileFormat.valueOf(file.getFileFormat().toUpperCase());
      } catch (Exception e) {
        format = FileFormat.PARQUET;
      }
      out.add(
          new DeleteFile(
              content,
              file.getFilePath(),
              format,
              file.getRecordCount(),
              file.getFileSizeInBytes(),
              file.getEqualityFieldIdsList(),
              Optional.empty(),
              Optional.empty(),
              0));
    }
    return out;
  }

  private static List<DeleteFile> selectDeleteFiles(
      List<DeleteFile> deleteFiles, List<Integer> deleteIndices) {
    if (deleteFiles == null || deleteFiles.isEmpty() || deleteIndices == null || deleteIndices.isEmpty()) {
      return List.of();
    }
    List<DeleteFile> out = new ArrayList<>(deleteIndices.size());
    for (Integer idx : deleteIndices) {
      if (idx == null) {
        continue;
      }
      int i = idx;
      if (i < 0 || i >= deleteFiles.size()) {
        continue;
      }
      out.add(deleteFiles.get(i));
    }
    return out;
  }

  private Map<String, String> buildStorageProperties() {
    Map<String, String> props = new java.util.HashMap<>();
    if (config.getS3AccessKey() != null && !config.getS3AccessKey().isBlank()) {
      props.put("s3.aws-access-key", config.getS3AccessKey());
    }
    if (config.getS3SecretKey() != null && !config.getS3SecretKey().isBlank()) {
      props.put("s3.aws-secret-key", config.getS3SecretKey());
    }
    if (config.getS3SessionToken() != null && !config.getS3SessionToken().isBlank()) {
      props.put("s3.aws-session-token", config.getS3SessionToken());
    }
    if (config.getS3Region() != null && !config.getS3Region().isBlank()) {
      props.put("s3.region", config.getS3Region());
    }
    if (config.getS3Endpoint() != null && !config.getS3Endpoint().isBlank()) {
      props.put("s3.endpoint", config.getS3Endpoint());
    }
    if (config.getS3StsRoleArn() != null && !config.getS3StsRoleArn().isBlank()) {
      props.put("s3.sts.role-arn", config.getS3StsRoleArn());
    }
    if (config.getS3StsRegion() != null && !config.getS3StsRegion().isBlank()) {
      props.put("s3.sts.region", config.getS3StsRegion());
    }
    if (config.getS3StsEndpoint() != null && !config.getS3StsEndpoint().isBlank()) {
      props.put("s3.sts.endpoint", config.getS3StsEndpoint());
    }
    if (config.getS3RoleSessionName() != null && !config.getS3RoleSessionName().isBlank()) {
      props.put("s3.role-session-name", config.getS3RoleSessionName());
    }
    return props;
  }

  private static void logDeleteFiles(List<ScanFile> files, String tableId) {
    if (files == null || files.isEmpty()) {
      return;
    }
  }
}

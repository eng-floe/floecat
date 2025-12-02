package ai.floedb.metacat.connector.iceberg.impl;

import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.FileColumnStats;
import ai.floedb.metacat.catalog.rpc.FileContent;
import ai.floedb.metacat.catalog.rpc.IcebergMetadata;
import ai.floedb.metacat.catalog.rpc.IcebergMetadataLogEntry;
import ai.floedb.metacat.catalog.rpc.IcebergRef;
import ai.floedb.metacat.catalog.rpc.IcebergSchema;
import ai.floedb.metacat.catalog.rpc.IcebergSnapshotLogEntry;
import ai.floedb.metacat.catalog.rpc.IcebergSortField;
import ai.floedb.metacat.catalog.rpc.IcebergSortOrder;
import ai.floedb.metacat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.TableStats;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.common.GenericStatsEngine;
import ai.floedb.metacat.connector.common.ProtoStatsBuilder;
import ai.floedb.metacat.connector.common.StatsEngine;
import ai.floedb.metacat.connector.common.ndv.ColumnNdv;
import ai.floedb.metacat.connector.common.ndv.FilteringNdvProvider;
import ai.floedb.metacat.connector.common.ndv.NdvProvider;
import ai.floedb.metacat.connector.common.ndv.ParquetNdvProvider;
import ai.floedb.metacat.connector.common.ndv.SamplingNdvProvider;
import ai.floedb.metacat.connector.common.ndv.StaticOnceNdvProvider;
import ai.floedb.metacat.connector.spi.ConnectorFormat;
import ai.floedb.metacat.connector.spi.MetacatConnector;
import ai.floedb.metacat.execution.rpc.ScanFile;
import ai.floedb.metacat.execution.rpc.ScanFileContent;
import ai.floedb.metacat.types.LogicalType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;

public final class IcebergConnector implements MetacatConnector {
  private final String connectorId;
  private final RESTCatalog catalog;
  private final GlueIcebergFilter glueFilter;
  private final Table singleTable;
  private final String singleNamespaceFq;
  private final String singleTableName;
  private final boolean ndvEnabled;
  private final double ndvSampleFraction;
  private final long ndvMaxFiles;

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(IcebergConnector.class);

  private IcebergConnector(
      String connectorId,
      RESTCatalog catalog,
      GlueIcebergFilter filter,
      Table singleTable,
      String singleNamespaceFq,
      String singleTableName,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles) {
    this.connectorId = connectorId;
    this.catalog = catalog;
    this.glueFilter = filter;
    this.singleTable = singleTable;
    this.singleNamespaceFq = singleNamespaceFq;
    this.singleTableName = singleTableName;
    this.ndvEnabled = ndvEnabled;
    this.ndvSampleFraction = ndvSampleFraction;
    this.ndvMaxFiles = ndvMaxFiles;
  }

  public static MetacatConnector create(
      String uri,
      Map<String, String> options,
      String authScheme,
      Map<String, String> authProps,
      Map<String, String> headerHints) {

    Objects.requireNonNull(uri, "uri");

    Map<String, String> opts = (options == null) ? Collections.emptyMap() : options;
    boolean ndvEnabled = parseNdvEnabled(options);
    double ndvSampleFraction = parseNdvSampleFraction(options);

    long ndvMaxFiles = 0L;
    if (options != null) {
      try {
        ndvMaxFiles = Long.parseLong(options.getOrDefault("stats.ndv.max_files", "0"));
        if (ndvMaxFiles < 0) {
          ndvMaxFiles = 0;
        }
      } catch (NumberFormatException ignore) {
      }
    }

    String externalMetadata = opts.get("external.metadata-location");
    if (externalMetadata != null && !externalMetadata.isBlank()) {
      Table table = loadExternalTable(externalMetadata, opts);
      String namespaceFq = opts.getOrDefault("external.namespace", "");
      String detectedName =
          (table.name() == null || table.name().isBlank())
              ? deriveTableName(externalMetadata)
              : table.name();
      String tableName = opts.getOrDefault("external.table-name", detectedName);
      return new IcebergConnector(
          "iceberg-filesystem",
          null,
          null,
          table,
          namespaceFq,
          tableName,
          ndvEnabled,
          ndvSampleFraction,
          ndvMaxFiles);
    }

    Map<String, String> props = new HashMap<>();
    props.put("type", "rest");
    props.put("uri", uri);
    if (!opts.isEmpty()) {
      for (var e : opts.entrySet()) {
        String k = e.getKey();
        if (k.startsWith("rest.") || k.startsWith("s3.") || k.equals("io-impl")) {
          props.put(k, e.getValue());
        }
      }
    }

    String scheme = (authScheme == null ? "none" : authScheme.trim().toLowerCase(Locale.ROOT));
    switch (scheme) {
      case "aws-sigv4" -> {
        String signingName = authProps.getOrDefault("signing-name", "glue");
        String signingRegion =
            authProps.getOrDefault(
                "signing-region",
                props.getOrDefault(
                    "rest.signing-region", props.getOrDefault("s3.region", "us-east-1")));
        props.put("rest.auth.type", "sigv4");
        props.put("rest.signing-name", signingName);
        props.put("rest.signing-region", signingRegion);

        props.putIfAbsent("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.putIfAbsent("s3.region", signingRegion);
      }

      case "oauth2" -> {
        String token =
            Objects.requireNonNull(authProps.get("token"), "authProps.token required for oauth2");
        props.put("token", token);
      }

      case "none" -> {}

      default -> throw new IllegalArgumentException("Unsupported auth scheme: " + authScheme);
    }

    if (headerHints != null) {
      headerHints.forEach((k, v) -> props.put("header." + k, v));
    }

    props.putIfAbsent("rest.client.user-agent", "metacat-connector-iceberg");

    var glue =
        GlueClient.builder()
            .region(Region.of(props.getOrDefault("s3.region", "us-east-1")))
            .build();

    RESTCatalog cat = new RESTCatalog();
    cat.initialize("metacat-iceberg", Collections.unmodifiableMap(props));

    return new IcebergConnector(
        "iceberg-rest",
        cat,
        new GlueIcebergFilter(glue),
        null,
        null,
        null,
        ndvEnabled,
        ndvSampleFraction,
        ndvMaxFiles);
  }

  @Override
  public String id() {
    return connectorId;
  }

  @Override
  public ConnectorFormat format() {
    return ConnectorFormat.CF_ICEBERG;
  }

  @Override
  public List<String> listNamespaces() {
    if (isSingleTableMode()) {
      return (singleNamespaceFq == null || singleNamespaceFq.isBlank())
          ? List.of()
          : List.of(singleNamespaceFq);
    }
    return catalog.listNamespaces().stream()
        .map(Namespace::toString)
        .filter(glueFilter::databaseHasIceberg)
        .sorted()
        .toList();
  }

  @Override
  public List<String> listTables(String namespaceFq) {
    if (isSingleTableMode()) {
      if (namespaceMatches(namespaceFq)) {
        return List.of(singleTableName);
      }
      return List.of();
    }
    return glueFilter.icebergTables(namespaceFq);
  }

  @Override
  public TableDescriptor describe(String namespaceFq, String tableName) {
    Table table = loadTable(namespaceFq, tableName);
    Schema schema = table.schema();
    String schemaJson = SchemaParser.toJson(schema);
    List<String> partitionKeys = table.spec().fields().stream().map(f -> f.name()).toList();

    return new TableDescriptor(
        namespaceFq, tableName, table.location(), schemaJson, partitionKeys, table.properties());
  }

  @Override
  public List<SnapshotBundle> enumerateSnapshotsWithStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      Set<String> includeColumns) {
    return enumerateSnapshotsWithStats(
        namespaceFq, tableName, destinationTableId, includeColumns, true);
  }

  @Override
  public List<SnapshotBundle> enumerateSnapshotsWithStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      Set<String> includeColumns,
      boolean includeStatistics) {
    Table table = loadTable(namespaceFq, tableName);

    final Set<Integer> includeIds;
    if (!includeStatistics) {
      includeIds = Set.of();
    } else if (includeColumns == null || includeColumns.isEmpty()) {
      includeIds =
          table.schema().columns().stream()
              .map(Types.NestedField::fieldId)
              .collect(Collectors.toCollection(LinkedHashSet::new));
    } else {
      includeIds = resolveFieldIdsNested(table.schema(), includeColumns);
    }

    IcebergMetadata icebergMetadata = buildIcebergMetadata(namespaceFq, tableName, table);

    List<SnapshotBundle> out = new ArrayList<>();
    for (Snapshot snapshot : table.snapshots()) {
      long snapshotId = snapshot.snapshotId();
      long parentId = snapshot.parentId() != null ? snapshot.parentId().longValue() : 0;
      long createdMs = snapshot.timestampMillis();

      int schemaId = (snapshot != null) ? snapshot.schemaId() : table.schema().schemaId();
      Schema schema = Optional.ofNullable(table.schemas().get(schemaId)).orElse(table.schema());
      String schemaJson = SchemaParser.toJson(schema);

      TableStats tStats = null;
      List<ColumnStats> cStats = List.of();
      List<FileColumnStats> fileStats = List.of();
      if (includeStatistics) {
        EngineOut engineOutput = runEngine(table, snapshotId, includeIds);

        tStats =
            ProtoStatsBuilder.toTableStats(
                destinationTableId,
                snapshotId,
                createdMs,
                TableFormat.TF_ICEBERG,
                engineOutput.result());
        cStats =
            ProtoStatsBuilder.toColumnStats(
                destinationTableId,
                snapshotId,
                TableFormat.TF_ICEBERG,
                engineOutput.result().columns(),
                id -> {
                  var f = table.schema().findField((Integer) id);
                  return f == null ? "" : f.name();
                },
                id -> {
                  var f = table.schema().findField((Integer) id);
                  return f == null ? null : IcebergTypeMapper.toLogical(f.type());
                },
                createdMs,
                engineOutput.result().totalRowCount());
        var baseFiles =
            ProtoStatsBuilder.toFileColumnStats(
                destinationTableId,
                snapshotId,
                TableFormat.TF_ICEBERG,
                engineOutput.result().files(),
                id -> {
                  var f = table.schema().findField((Integer) id);
                  return f == null ? "" : f.name();
                },
                id -> {
                  var f = table.schema().findField((Integer) id);
                  return f == null ? null : IcebergTypeMapper.toLogical(f.type());
                },
                createdMs);

        List<FileColumnStats> deleteStats = new ArrayList<>();
        TableScan scan = table.newScan().useSnapshot(snapshotId);
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
          for (FileScanTask task : tasks) {
            for (var df : task.deletes()) {
              List<Integer> eqIds =
                  df.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES
                      ? df.equalityFieldIds()
                      : List.of();
              deleteStats.add(
                  FileColumnStats.newBuilder()
                      .setTableId(destinationTableId)
                      .setSnapshotId(snapshotId)
                      .setFilePath(df.location())
                      .setRowCount(df.recordCount())
                      .setSizeBytes(df.fileSizeInBytes())
                      .addAllEqualityFieldIds(eqIds)
                      .setFileContent(
                          df.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES
                              ? FileContent.FC_EQUALITY_DELETES
                              : FileContent.FC_POSITION_DELETES)
                      .build());
            }
          }
        } catch (Exception e) {
          throw new RuntimeException(
              "Failed to enumerate delete files for snapshot " + snapshotId, e);
        }

        List<FileColumnStats> allFiles = new ArrayList<>(baseFiles);
        allFiles.addAll(deleteStats);
        fileStats = allFiles;
      }

      Map<String, String> summary =
          snapshot.summary() == null ? Map.of() : Map.copyOf(snapshot.summary());
      String manifestList = snapshot.manifestListLocation();
      long sequenceNumber = snapshot.sequenceNumber();

      out.add(
          new SnapshotBundle(
              snapshotId,
              parentId,
              createdMs,
              tStats,
              cStats,
              fileStats,
              schemaJson,
              toPartitionSpecInfo(table, snapshot),
              sequenceNumber,
              manifestList,
              summary,
              schemaId,
              icebergMetadata));
    }
    return out;
  }

  @Override
  public ScanBundle plan(String namespaceFq, String tableName, long snapshotId, long asOfTime) {
    Table table = loadTable(namespaceFq, tableName);

    TableScan scan = table.newScan().includeColumnStats();
    if (snapshotId > 0) {
      scan.useSnapshot(snapshotId);
    } else {
      scan.asOfTime(asOfTime);
    }

    Snapshot snap = table.snapshot(snapshotId);
    int schemaId = (snap != null) ? snap.schemaId() : table.schema().schemaId();
    Schema schema = Optional.ofNullable(table.schemas().get(schemaId)).orElse(table.schema());
    var schemaColumns = schema.columns();

    ScanBundle result = new ScanBundle(new ArrayList<>(), new ArrayList<>());
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        {
          DataFile df = task.file();
          var pf =
              ScanFile.newBuilder()
                  .setFilePath(df.location())
                  .setFileFormat(df.format().name())
                  .setFileSizeInBytes(df.fileSizeInBytes())
                  .setRecordCount(df.recordCount())
                  .setPartitionDataJson(partitionJson(table, df))
                  .setPartitionSpecId(df.specId())
                  .setFileContent(ScanFileContent.SCAN_FILE_CONTENT_DATA);
          result.dataFiles().add(pf.build());
        }

        for (var df : task.deletes()) {
          var pf =
              ScanFile.newBuilder()
                  .setFilePath(df.location())
                  .setFileFormat(df.format().name())
                  .setFileSizeInBytes(df.fileSizeInBytes())
                  .setRecordCount(df.recordCount())
                  .setPartitionDataJson(partitionJson(table, df))
                  .setPartitionSpecId(df.specId())
                  .setFileContent(
                      df.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES
                          ? ScanFileContent.SCAN_FILE_CONTENT_EQUALITY_DELETES
                          : ScanFileContent.SCAN_FILE_CONTENT_POSITION_DELETES);
          result.deleteFiles().add(pf.build());
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Iceberg planning failed (snapshot " + snapshotId + ")", e);
    }

    return result;
  }

  private boolean isSingleTableMode() {
    return singleTable != null;
  }

  private boolean namespaceMatches(String namespaceFq) {
    String stored = singleNamespaceFq == null ? "" : singleNamespaceFq;
    String incoming = namespaceFq == null ? "" : namespaceFq;
    return stored.equals(incoming);
  }

  private Table loadTable(String namespaceFq, String tableName) {
    if (isSingleTableMode()) {
      return singleTable;
    }
    Namespace namespace =
        (namespaceFq == null || namespaceFq.isBlank())
            ? Namespace.empty()
            : Namespace.of(namespaceFq.split("\\."));
    TableIdentifier tableId =
        namespace.isEmpty()
            ? TableIdentifier.of(tableName)
            : TableIdentifier.of(namespace, tableName);
    return catalog.loadTable(tableId);
  }

  private Long safeLong(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  private static Table loadExternalTable(String metadataLocation, Map<String, String> options) {
    Configuration conf = new Configuration();
    if (options != null) {
      options.forEach(
          (k, v) -> {
            if (k.startsWith("fs.") || k.startsWith("hadoop.") || k.startsWith("s3.")) {
              conf.set(k, v);
            }
          });
    }
    HadoopTables tables = new HadoopTables(conf);
    return tables.load(metadataLocation);
  }

  private static String deriveTableName(String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return "table";
    }
    String path = metadataLocation;
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    int slash = path.lastIndexOf('/');
    if (slash >= 0 && slash + 1 < path.length()) {
      path = path.substring(slash + 1);
    }
    if (path.endsWith(".json")) {
      path = path.substring(0, path.length() - 5);
    }
    return path.isBlank() ? "table" : path;
  }

  private String partitionJson(Table table, ContentFile<?> file) {
    PartitionSpec spec = table.specs().getOrDefault(file.specId(), table.spec());
    StructLike partition = file.partition();
    if (spec == null || spec.fields().isEmpty() || partition == null) {
      return "{\"partitionValues\":[]}";
    }
    try {
      List<Map<String, Object>> values = new ArrayList<>(spec.fields().size());
      for (int i = 0; i < spec.fields().size(); i++) {
        PartitionField field = spec.fields().get(i);
        Object val = partition.get(i, Object.class);
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("id", field.sourceId());
        entry.put("value", val);
        values.add(entry);
      }
      Map<String, Object> root = new LinkedHashMap<>();
      root.put("partitionValues", values);
      return new ObjectMapper().writeValueAsString(root);
    } catch (Exception e) {
      return "";
    }
  }

  private PartitionSpecInfo toPartitionSpecInfo(Table table, Snapshot snapshot) {
    if (snapshot == null) {
      return null;
    }

    Map<Integer, PartitionSpec> specs = table.specs();
    Integer snapshotSpecId = null;
    TableScan scan = table.newScan().useSnapshot(snapshot.snapshotId());
    CloseableIterable<FileScanTask> tasks = scan.planFiles();
    Set<Integer> specSet = new LinkedHashSet<>();
    for (FileScanTask task : tasks) {
      specSet.add(task.file().specId());
    }

    if (specSet.size() == 1) {
      snapshotSpecId = specSet.iterator().next();
    }

    if (snapshotSpecId == null && table.spec() != null) {
      snapshotSpecId = table.spec().specId();
    }

    PartitionSpec spec = snapshotSpecId == null ? null : specs.get(snapshotSpecId);
    if (spec == null) {
      return null;
    }

    return toPartitionSpecInfo(spec);
  }

  private PartitionSpecInfo toPartitionSpecInfo(PartitionSpec spec) {
    if (spec == null) {
      return null;
    }
    String specName = spec.isUnpartitioned() ? "unpartitioned" : "spec-" + spec.specId();
    PartitionSpecInfo.Builder builder =
        PartitionSpecInfo.newBuilder().setSpecId(spec.specId()).setSpecName(specName);
    for (PartitionField field : spec.fields()) {
      builder.addFields(
          ai.floedb.metacat.catalog.rpc.PartitionField.newBuilder()
              .setFieldId(field.sourceId())
              .setName(field.name())
              .setTransform(field.transform().toString())
              .build());
    }
    return builder.build();
  }

  private IcebergMetadata buildIcebergMetadata(String namespaceFq, String tableName, Table table) {
    TableMetadata metadata = tableMetadata(table);
    if (metadata == null && catalog != null) {
      try {
        Namespace namespace =
            (namespaceFq == null || namespaceFq.isBlank())
                ? Namespace.empty()
                : Namespace.of(namespaceFq.split("\\."));
        TableIdentifier identifier =
            namespace.isEmpty()
                ? TableIdentifier.of(tableName)
                : TableIdentifier.of(namespace, tableName);
        Table restTable = catalog.loadTable(identifier);
        metadata = tableMetadata(restTable);
      } catch (Exception e) {
        System.out.println(
            "[IcebergConnector] failed to fetch metadata via REST catalog for "
                + namespaceFq
                + "."
                + tableName
                + ": "
                + e);
      }
    }
    if (metadata == null) {
      String fallbackLocation = tableMetadataLocation(table);
      if ((fallbackLocation == null || fallbackLocation.isBlank())
          && glueFilter != null
          && namespaceFq != null
          && !namespaceFq.isBlank()
          && tableName != null
          && !tableName.isBlank()) {
        try {
          fallbackLocation = glueFilter.metadataLocation(namespaceFq, tableName);
        } catch (Exception e) {
          System.out.println(
              "[IcebergConnector] failed to fetch metadata location from Glue for "
                  + namespaceFq
                  + "."
                  + tableName
                  + ": "
                  + e);
        }
      }
      if (fallbackLocation == null || fallbackLocation.isBlank()) {
        return null;
      }
      IcebergMetadata.Builder minimal =
          IcebergMetadata.newBuilder()
              .setMetadataLocation(fallbackLocation)
              .setFormatVersion(2)
              .setTableUuid(
                  table
                      .properties()
                      .getOrDefault("table-uuid", tableName == null ? "" : tableName));
      Optional.ofNullable(table.properties().get("current-snapshot-id"))
          .map(this::safeLong)
          .ifPresent(minimal::setCurrentSnapshotId);
      return minimal.build();
    }
    return toIcebergMetadata(metadata);
  }

  private TableMetadata tableMetadata(Table table) {
    if (!(table instanceof HasTableOperations hasOps)) {
      return null;
    }
    return hasOps.operations().current();
  }

  private String tableMetadataLocation(Table table) {
    Map<String, String> props = table.properties();
    if (props == null || props.isEmpty()) {
      return null;
    }
    String loc = props.get("metadata-location");
    if (loc == null || loc.isBlank()) {
      loc = props.get("metadata_location");
    }
    return loc;
  }

  private IcebergMetadata toIcebergMetadata(TableMetadata metadata) {
    IcebergMetadata.Builder builder =
        IcebergMetadata.newBuilder()
            .setTableUuid(metadata.uuid())
            .setFormatVersion(metadata.formatVersion())
            .setMetadataLocation(metadata.metadataFileLocation())
            .setLastUpdatedMs(metadata.lastUpdatedMillis())
            .setLastColumnId(metadata.lastColumnId())
            .setCurrentSchemaId(metadata.currentSchemaId())
            .setDefaultSpecId(metadata.defaultSpecId())
            .setLastPartitionId(metadata.lastAssignedPartitionId())
            .setDefaultSortOrderId(metadata.defaultSortOrderId())
            .setLastSequenceNumber(metadata.lastSequenceNumber());

    Snapshot currentSnapshot = metadata.currentSnapshot();
    if (currentSnapshot != null) {
      builder.setCurrentSnapshotId(currentSnapshot.snapshotId());
    }

    if (metadata.schemas() != null) {
      for (Schema schema : metadata.schemas()) {
        builder.addSchemas(
            IcebergSchema.newBuilder()
                .setSchemaId(schema.schemaId())
                .setSchemaJson(SchemaParser.toJson(schema))
                .addAllIdentifierFieldIds(schema.identifierFieldIds())
                .setLastColumnId(schema.highestFieldId())
                .build());
      }
    }

    if (metadata.specsById() != null) {
      for (PartitionSpec spec : metadata.specsById().values()) {
        PartitionSpecInfo info = toPartitionSpecInfo(spec);
        if (info != null) {
          builder.addPartitionSpecs(info);
        }
      }
    }

    if (metadata.sortOrders() != null) {
      for (SortOrder order : metadata.sortOrders()) {
        IcebergSortOrder.Builder orderBuilder =
            IcebergSortOrder.newBuilder().setSortOrderId(order.orderId());
        for (SortField field : order.fields()) {
          orderBuilder.addFields(
              IcebergSortField.newBuilder()
                  .setSourceFieldId(field.sourceId())
                  .setTransform(
                      field.transform() == null ? "identity" : field.transform().toString())
                  .setDirection(field.direction().name())
                  .setNullOrder(field.nullOrder().name())
                  .build());
        }
        builder.addSortOrders(orderBuilder.build());
      }
    }

    for (HistoryEntry entry : metadata.snapshotLog()) {
      builder.addSnapshotLog(
          IcebergSnapshotLogEntry.newBuilder()
              .setSnapshotId(entry.snapshotId())
              .setTimestampMs(entry.timestampMillis())
              .build());
    }

    for (MetadataLogEntry entry : metadata.previousFiles()) {
      builder.addMetadataLog(
          IcebergMetadataLogEntry.newBuilder()
              .setFile(entry.file())
              .setTimestampMs(entry.timestampMillis())
              .build());
    }
    builder.addMetadataLog(
        IcebergMetadataLogEntry.newBuilder()
            .setFile(metadata.metadataFileLocation())
            .setTimestampMs(metadata.lastUpdatedMillis())
            .build());

    metadata
        .refs()
        .forEach(
            (name, ref) -> {
              IcebergRef.Builder refBuilder =
                  IcebergRef.newBuilder()
                      .setSnapshotId(ref.snapshotId())
                      .setType(ref.type().name());
              if (ref.maxRefAgeMs() != null) {
                refBuilder.setMaxReferenceAgeMs(ref.maxRefAgeMs());
              }
              if (ref.maxSnapshotAgeMs() != null) {
                refBuilder.setMaxSnapshotAgeMs(ref.maxSnapshotAgeMs());
              }
              if (ref.minSnapshotsToKeep() != null) {
                refBuilder.setMinSnapshotsToKeep(ref.minSnapshotsToKeep());
              }
              builder.putRefs(name, refBuilder.build());
            });

    return builder.build();
  }

  @Override
  public void close() {
    try {
      catalog.close();
    } catch (Exception ignore) {
    }
  }

  private record EngineOut(StatsEngine.Result<Integer> result) {}

  private EngineOut runEngine(Table table, long snapshotId, Set<Integer> colIds) {
    try (var planner = new IcebergPlanner(table, snapshotId, colIds, null)) {

      Map<Integer, String> colNames = planner.columnNamesByKey();
      Map<Integer, LogicalType> logicalTypes = planner.logicalTypesByKey();

      if (!ndvEnabled) {
        NdvProvider none = null;
        StatsEngine<Integer> engine =
            new GenericStatsEngine<>(planner, none, null, colNames, logicalTypes);
        var result = engine.compute();
        return new EngineOut(result);
      }

      Map<String, ColumnNdv> puffinMap = null;
      try {
        puffinMap = PuffinNdvProvider.readPuffinNdvWithSketches(table, snapshotId, colNames::get);
      } catch (IOException ioe) {
      }

      NdvProvider bootstrap =
          (puffinMap == null || puffinMap.isEmpty()) ? null : new StaticOnceNdvProvider(puffinMap);

      final int totalCols = colNames.size();
      int missing;
      if (bootstrap != null) {
        final Map<String, ColumnNdv> m = puffinMap;
        int miss = 0;
        for (String columnName : colNames.values()) {
          ColumnNdv ndv = m.get(columnName);
          boolean hasData =
              ndv != null
                  && (ndv.approx != null || (ndv.sketches != null && !ndv.sketches.isEmpty()));
          if (!hasData) miss++;
        }
        missing = miss;
      } else {
        missing = totalCols;
      }

      NdvProvider perFileNdv = null;
      if (missing > 0) {
        var parquetNdv = ParquetNdvProvider.forIcebergIO(path -> table.io().newInputFile(path));
        NdvProvider base = FilteringNdvProvider.bySuffix(Set.of(".parquet", ".parq"), parquetNdv);

        if (ndvSampleFraction < 1.0 || ndvMaxFiles > 0) {
          base = new SamplingNdvProvider(base, ndvSampleFraction, ndvMaxFiles);
        }

        perFileNdv = base;
      }

      var engine = new GenericStatsEngine<>(planner, perFileNdv, bootstrap, colNames, logicalTypes);

      var result = engine.compute();
      return new EngineOut(result);
    } catch (Exception e) {
      throw new RuntimeException("Stats compute failed for snapshot " + snapshotId, e);
    }
  }

  private static Set<Integer> resolveFieldIdsNested(Schema schema, Set<String> selectors) {
    Map<String, Integer> byPath = new LinkedHashMap<>();
    for (Types.NestedField top : schema.columns()) {
      collectNested(top, "", byPath);
    }

    Set<Integer> out = new LinkedHashSet<>();
    for (String sel : selectors) {
      if (sel == null || sel.isBlank()) {
        continue;
      }

      String s = sel.trim();
      if (s.startsWith("#")) {
        out.add(Integer.parseInt(s.substring(1)));
      } else {
        Integer id = byPath.get(s);
        if (id == null) {
          throw new IllegalArgumentException("Unknown column selector: " + s);
        }
        out.add(id);
      }
    }
    return out;
  }

  private static void collectNested(Types.NestedField f, String prefix, Map<String, Integer> out) {
    final String name = prefix.isEmpty() ? f.name() : prefix + "." + f.name();
    final Type t = f.type();

    out.put(name, f.fieldId());

    if (t.isStructType()) {
      for (Types.NestedField child : t.asStructType().fields()) {
        collectNested(child, name, out);
      }
    } else if (t.isListType()) {
      Types.ListType lt = t.asListType();
      Types.NestedField elem = lt.fields().get(0);
      collectNested(elem, name, out);
    } else if (t.isMapType()) {
      Types.MapType mt = t.asMapType();
      Types.NestedField key = mt.fields().get(0);
      Types.NestedField val = mt.fields().get(1);
      collectNested(key, name, out);
      collectNested(val, name, out);
    }
  }

  private static boolean parseNdvEnabled(Map<String, String> options) {
    if (options != null) {
      String v = options.getOrDefault("stats.ndv.enabled", "true");
      return !v.equalsIgnoreCase("false");
    }
    return false;
  }

  private static double parseNdvSampleFraction(Map<String, String> options) {
    if (options == null) {
      return 0.0d;
    }

    String raw = options.get("stats.ndv.sample_fraction");
    if (raw == null || raw.isBlank()) {
      return 1.0d;
    }
    try {
      double f = Double.parseDouble(raw.trim());
      if (f <= 0.0d) {
        return 0.0d;
      }
      if (f > 1.0d) {
        return 1.0d;
      }
      return f;
    } catch (NumberFormatException nfe) {
      return 1.0d;
    }
  }

  final class NdvOnlyResult<K> implements StatsEngine.Result<K> {
    private final long rows;
    private final long bytes;
    private final long files;
    private final Map<K, StatsEngine.ColumnAgg> cols;

    NdvOnlyResult(long rows, long bytes, long files, Map<K, StatsEngine.ColumnAgg> cols) {
      this.rows = rows;
      this.bytes = bytes;
      this.files = files;
      this.cols = cols;
    }

    @Override
    public long totalRowCount() {
      return rows;
    }

    @Override
    public long totalSizeBytes() {
      return bytes;
    }

    @Override
    public long fileCount() {
      return files;
    }

    @Override
    public Map<K, StatsEngine.ColumnAgg> columns() {
      return cols;
    }

    static <K> StatsEngine.ColumnAgg ndvOnly(ColumnNdv ndv) {
      return new StatsEngine.ColumnAgg() {
        @Override
        public Long ndvExact() {
          return null;
        }

        @Override
        public ColumnNdv ndv() {
          return ndv;
        }

        @Override
        public Long valueCount() {
          return null;
        }

        @Override
        public Long nullCount() {
          return null;
        }

        @Override
        public Long nanCount() {
          return null;
        }

        @Override
        public Object min() {
          return null;
        }

        @Override
        public Object max() {
          return null;
        }
      };
    }
  }
}

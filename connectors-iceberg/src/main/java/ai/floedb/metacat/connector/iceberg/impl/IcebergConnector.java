package ai.floedb.metacat.connector.iceberg.impl;

import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.common.GenericStatsEngine;
import ai.floedb.metacat.connector.common.StatsEngine;
import ai.floedb.metacat.connector.common.ndv.ColumnNdv;
import ai.floedb.metacat.connector.common.ndv.FilteringNdvProvider;
import ai.floedb.metacat.connector.common.ndv.NdvProvider;
import ai.floedb.metacat.connector.common.ndv.ParquetNdvProvider;
import ai.floedb.metacat.connector.common.ndv.StaticOnceNdvProvider;
import ai.floedb.metacat.connector.spi.ConnectorFormat;
import ai.floedb.metacat.connector.spi.MetacatConnector;
import ai.floedb.metacat.types.LogicalType;
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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;

public final class IcebergConnector implements MetacatConnector {
  private final String connectorId;
  private final RESTCatalog catalog;
  private final GlueIcebergFilter glueFilter;

  private IcebergConnector(String connectorId, RESTCatalog catalog, GlueIcebergFilter filter) {
    this.connectorId = connectorId;
    this.catalog = catalog;
    this.glueFilter = filter;
  }

  public static MetacatConnector create(
      String uri,
      Map<String, String> options,
      String authScheme,
      Map<String, String> authProps,
      Map<String, String> headerHints) {

    Objects.requireNonNull(uri, "uri");

    Map<String, String> props = new HashMap<>();
    props.put("type", "rest");
    props.put("uri", uri);

    if (options != null) {
      for (var e : options.entrySet()) {
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

    return new IcebergConnector("iceberg-rest", cat, new GlueIcebergFilter(glue));
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
    return catalog.listNamespaces().stream()
        .map(Namespace::toString)
        .filter(glueFilter::databaseHasIceberg)
        .sorted()
        .toList();
  }

  @Override
  public List<String> listTables(String namespaceFq) {
    return glueFilter.icebergTables(namespaceFq);
  }

  @Override
  public TableDescriptor describe(String namespaceFq, String tableName) {
    Namespace namespace = Namespace.of(namespaceFq.split("\\."));
    TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
    Table table = catalog.loadTable(tableId);
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
    Namespace ns = Namespace.of(namespaceFq.split("\\."));
    TableIdentifier tid = TableIdentifier.of(ns, tableName);
    Table table = catalog.loadTable(tid);

    final Set<Integer> includeIds =
        (includeColumns == null || includeColumns.isEmpty())
            ? table.schema().columns().stream()
                .map(Types.NestedField::fieldId)
                .collect(Collectors.toCollection(LinkedHashSet::new))
            : resolveFieldIdsNested(table.schema(), includeColumns);

    List<SnapshotBundle> out = new ArrayList<>();
    for (Snapshot snapshot : table.snapshots()) {
      long snapshotId = snapshot.snapshotId();
      long parentId = snapshot.parentId() != null ? snapshot.parentId().longValue() : 0;
      long createdMs = snapshot.timestampMillis();

      EngineOut engineOutput = runEngine(table, snapshotId, includeIds);

      var tStats =
          ProtoStatsBuilder.toTableStats(
              destinationTableId,
              snapshotId,
              createdMs,
              TableFormat.TF_ICEBERG,
              engineOutput.result());
      var cStats =
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

      out.add(new SnapshotBundle(snapshotId, parentId, createdMs, tStats, cStats));
    }
    return out;
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
    var colNames = buildColumnNameMap(table.schema(), colIds);
    var logicalTypes = buildLogicalTypeMap(table.schema(), colIds);

    Map<String, ColumnNdv> puffinMap = null;
    try {
      puffinMap = PuffinNdvProvider.readPuffinNdvWithSketches(table, snapshotId, colNames::get);
    } catch (IOException ioe) {
      // ignore
    }

    NdvProvider bootstrap =
        (puffinMap == null || puffinMap.isEmpty()) ? null : new StaticOnceNdvProvider(puffinMap);

    int missing = 0;
    final int totalCols = colNames.size();

    if (bootstrap != null) {
      final Map<String, ColumnNdv> puffinMapFinal = puffinMap;
      for (String columnName : colNames.values()) {
        ColumnNdv columnNdv = puffinMapFinal.get(columnName);
        boolean hasData =
            columnNdv != null
                && (columnNdv.approx != null
                    || (columnNdv.sketches != null && !columnNdv.sketches.isEmpty()));
        if (!hasData) {
          missing++;
        }
      }
    } else {
      missing = totalCols;
    }

    NdvProvider perFileNdv = null;
    if (missing > 0) {
      var parquetNdv = new ParquetNdvProvider(path -> table.io().newInputFile(path));
      perFileNdv = FilteringNdvProvider.bySuffix(Set.of(".parquet", ".parq"), parquetNdv);
    }

    try (var planner = new IcebergPlanner(table, snapshotId, colIds, perFileNdv)) {
      var engine = new GenericStatsEngine<>(planner, perFileNdv, bootstrap, colNames, logicalTypes);
      var result = engine.compute();
      return new EngineOut(result);
    } catch (Exception e) {
      throw new RuntimeException("Stats compute failed for snapshot " + snapshotId, e);
    }
  }

  private static Map<Integer, String> buildColumnNameMap(Schema schema, Set<Integer> includeIds) {
    return schema.columns().stream()
        .filter(f -> includeIds == null || includeIds.contains(f.fieldId()))
        .collect(Collectors.toUnmodifiableMap(Types.NestedField::fieldId, Types.NestedField::name));
  }

  private static Map<Integer, LogicalType> buildLogicalTypeMap(
      Schema schema, Set<Integer> includeIds) {
    return schema.columns().stream()
        .filter(f -> includeIds == null || includeIds.contains(f.fieldId()))
        .map(f -> Map.entry(f.fieldId(), IcebergTypeMapper.toLogical(f.type())))
        .filter(e -> e.getValue() != null)
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
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

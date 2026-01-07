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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.common.rpc.Operator;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.execution.rpc.ScanBundle;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.query.rpc.*;

import com.google.protobuf.Timestamp;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;

import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;

import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimeZoneKey;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class FloecatSplitManagerTest {

    private static Server server;
    private static ManagedChannel channel;

    private static LifecycleStub lifecycleStub;
    private static SchemaStub schemaStub;
    private static ScanStub scanStub;
    private static DirectoryStub directoryStub;

    private static QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub schema;
    private static QueryScanServiceGrpc.QueryScanServiceBlockingStub scans;
    private static QueryServiceGrpc.QueryServiceBlockingStub lifecycle;
    private static DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

    @BeforeAll
    static void startServer() throws Exception {
        lifecycleStub = new LifecycleStub();
        schemaStub = new SchemaStub();
        scanStub = new ScanStub();
        directoryStub = new DirectoryStub();

        String serverName = InProcessServerBuilder.generateName();
        server =
            InProcessServerBuilder.forName(serverName)
                .addService(lifecycleStub)
                .addService(schemaStub)
                .addService(scanStub)
                .addService(directoryStub)
                .build()
                .start();

        channel = InProcessChannelBuilder.forName(serverName).build();
        lifecycle = QueryServiceGrpc.newBlockingStub(channel);
        scans = QueryScanServiceGrpc.newBlockingStub(channel);
        schema = QuerySchemaServiceGrpc.newBlockingStub(channel);
        directory = DirectoryServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void shutdown() {
        if (channel != null) channel.shutdownNow();
        if (server != null) server.shutdownNow();
    }

    @AfterEach
    void resetStub() {
        scanStub.dataFileFormat = "PARQUET";
        scanStub.partitionDataJson = null;
        lifecycleStub.lastBegin = null;
        schemaStub.lastDescribe = null;
        scanStub.lastFetch = null;
    }

    @Test
    void includesSnapshotIdInBeginQuery() {
        FloecatSplitManager splitManager =
          new FloecatSplitManager(
              lifecycle,
              scans,
              schema,
              directory,
              new CatalogName("test-catalog"),
              new FloecatConfig());

        FloecatTableHandle handle =
            new FloecatTableHandle(
                new SchemaTableName("s", "t"),
                "tbl",
                "account",
                ResourceKind.RK_TABLE.name(),
                "s3://bucket/table",
                "{}",
                null,
                "TF_ICEBERG",
                "catalog",
                TupleDomain.all(),
                Set.of("col1"),
                123L,
                null);

        ConnectorSession session =
            new SimpleSession(Map.of(
                FloecatSessionProperties.SNAPSHOT_ID, 123L,
                FloecatSessionProperties.AS_OF_EPOCH_MILLIS, -1L));

        try {
            splitManager.getSplits(null, session, handle, DynamicFilter.EMPTY, Constraint.alwaysTrue());
        } catch (Throwable t) {
            assumeProcessAccessible(t);
            throw t;
        }

        QueryInput input = schemaStub.lastDescribe.getInputs(0);
        assertTrue(input.hasSnapshot());
        assertEquals(123L, input.getSnapshot().getSnapshotId());

        ResourceId expectedCatalogId = ResourceId.newBuilder()
            .setId("catalog-id")
            .setAccountId("account")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
        assertEquals(expectedCatalogId, lifecycleStub.lastBegin.getDefaultCatalogId());
    }

    @Test
    void includesAsOfInBeginQuery() {
        FloecatSplitManager splitManager =
            new FloecatSplitManager(
                lifecycle,
                scans,
                schema,
                directory,
                new CatalogName("test-catalog"),
                new FloecatConfig());

        long asOf = 1_700_000_000_000L;

        FloecatTableHandle handle =
            new FloecatTableHandle(
                new SchemaTableName("s", "t"),
                "tbl",
                "account",
                ResourceKind.RK_TABLE.name(),
                "s3://bucket/table",
                "{}",
                null,
                "TF_ICEBERG",
                "catalog",
                TupleDomain.all(),
                Set.of("col1"),
                null,
                asOf);

        ConnectorSession session =
            new SimpleSession(Map.of(
                FloecatSessionProperties.SNAPSHOT_ID, -1L,
                FloecatSessionProperties.AS_OF_EPOCH_MILLIS, asOf));

        try {
            splitManager.getSplits(null, session, handle, DynamicFilter.EMPTY, Constraint.alwaysTrue());
        } catch (Throwable t) {
            assumeProcessAccessible(t);
            throw t;
        }

        QueryInput input = schemaStub.lastDescribe.getInputs(0);
        Timestamp ts = input.getSnapshot().getAsOf();
        long millis = ts.getSeconds() * 1000L + ts.getNanos() / 1_000_000;

        assertEquals(asOf, millis);

        ResourceId expectedCatalogId = ResourceId.newBuilder()
            .setId("catalog-id")
            .setAccountId("account")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
        assertEquals(expectedCatalogId, lifecycleStub.lastBegin.getDefaultCatalogId());
    }

    @Test
    void buildsPredicatesAndRequiredColumnsFromDomains() throws Exception {
        FloecatSplitManager splitManager =
            new FloecatSplitManager(
                lifecycle,
                scans,
                schema,
                directory,
                new CatalogName("test-catalog"),
                new FloecatConfig());

        IcebergColumnHandle idCol =
            new IcebergColumnHandle(
                ColumnIdentity.primitiveColumnIdentity(1, "id"),
                BigintType.BIGINT,
                List.of(),
                BigintType.BIGINT,
                false,
                Optional.empty());

        IcebergColumnHandle bucketCol =
            new IcebergColumnHandle(
                ColumnIdentity.primitiveColumnIdentity(2, "bucket"),
                BigintType.BIGINT,
                List.of(),
                BigintType.BIGINT,
                false,
                Optional.empty());

        TupleDomain<IcebergColumnHandle> enforced =
            TupleDomain.withColumnDomains(Map.of(idCol, Domain.singleValue(BigintType.BIGINT, 7L)));

        FloecatTableHandle handle =
            new FloecatTableHandle(
                new SchemaTableName("s", "t"),
                "tbl",
                "account",
                ResourceKind.RK_TABLE.name(),
                "s3://bucket/table",
                "{}",
                null,
                "TF_ICEBERG",
                "catalog",
                enforced,
                Set.of("projected"),
                123L,
                null);

        Map<ColumnHandle, Domain> constraintDomains =
            Map.of((ColumnHandle) bucketCol,
                Domain.create(ValueSet.ofRanges(Range.greaterThan(BigintType.BIGINT, 4L)), false));

        Constraint constraint =
            new Constraint(TupleDomain.withColumnDomains(constraintDomains));

        ConnectorSession session =
            new SimpleSession(Map.of(
                FloecatSessionProperties.SNAPSHOT_ID, 123L,
                FloecatSessionProperties.AS_OF_EPOCH_MILLIS, -1L));

        ConnectorSplitSource splitSource;

        try {
            splitSource =
                splitManager.getSplits(null, session, handle, DynamicFilter.EMPTY, constraint);
        } catch (Throwable t) {
            assumeProcessAccessible(t);
            throw t;
        }

        assertTrue(
            scanStub.lastFetch.getRequiredColumnsList()
                .containsAll(List.of("id", "bucket", "projected")));

        assertEquals(4, scanStub.lastFetch.getPredicatesCount());

        assertTrue(
            scanStub.lastFetch.getPredicatesList().stream()
                .anyMatch(p -> p.getColumn().equals("id")
                    && p.getOp() == Operator.OP_EQ));

        assertTrue(
            scanStub.lastFetch.getPredicatesList().stream()
                .anyMatch(p -> p.getColumn().equals("bucket")
                    && (p.getOp() == Operator.OP_GT || p.getOp() == Operator.OP_IS_NOT_NULL)));

        List<ConnectorSplit> splits =
            splitSource.getNextBatch(10).get().getSplits();
        assertEquals(1, splits.size());

        ResourceId expectedCatalogId = ResourceId.newBuilder()
            .setId("catalog-id")
            .setAccountId("account")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
        assertEquals(expectedCatalogId, lifecycleStub.lastBegin.getDefaultCatalogId());
    }

    @Test
    void defaultsFormatAndPartitionDataWhenMissing() throws Exception {
        scanStub.dataFileFormat = "ORC";
        scanStub.partitionDataJson = "";

        FloecatSplitManager splitManager =
            new FloecatSplitManager(
                lifecycle,
                scans,
                schema,
                directory,
                new CatalogName("test-catalog"),
                new FloecatConfig());

        FloecatTableHandle handle =
            new FloecatTableHandle(
                new SchemaTableName("s", "t"),
                "tbl",
                "account",
                ResourceKind.RK_TABLE.name(),
                "s3://bucket/table",
                "{}",
                null,
                "TF_ICEBERG",
                "catalog",
                TupleDomain.all(),
                Set.of(),
                null,
                null);

        ConnectorSession session =
            new SimpleSession(Map.of(
                FloecatSessionProperties.SNAPSHOT_ID, -1L,
                FloecatSessionProperties.AS_OF_EPOCH_MILLIS, -1L));

        ConnectorSplitSource splitSource;

        try {
            splitSource =
                splitManager.getSplits(null, session, handle, DynamicFilter.EMPTY, Constraint.alwaysTrue());
        } catch (Throwable t) {
            assumeProcessAccessible(t);
            throw t;
        }

        ConnectorSplit split =
            splitSource.getNextBatch(10).get().getSplits().getFirst();

        io.trino.plugin.iceberg.IcebergSplit iceberg =
            (io.trino.plugin.iceberg.IcebergSplit) split;

        assertEquals("{\"partitionValues\":[]}", iceberg.getPartitionDataJson());
        assertEquals(IcebergFileFormat.ORC, iceberg.getFileFormat());

        ResourceId expectedCatalogId = ResourceId.newBuilder()
            .setId("catalog-id")
            .setAccountId("account")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
        assertEquals(expectedCatalogId, lifecycleStub.lastBegin.getDefaultCatalogId());
    }

    private static class SimpleSession implements ConnectorSession {
        private final Map<String, Object> props;
        SimpleSession(Map<String, Object> props) { this.props = props; }
        @Override public String getQueryId() { return "query"; }
        @Override public Optional<String> getSource() { return Optional.empty(); }
        @Override public ConnectorIdentity getIdentity() { return ConnectorIdentity.ofUser("user"); }
        @Override public TimeZoneKey getTimeZoneKey() { return TimeZoneKey.UTC_KEY; }
        @Override public Locale getLocale() { return Locale.US; }
        @Override public Optional<String> getTraceToken() { return Optional.empty(); }
        @Override public Instant getStart() { return Instant.EPOCH; }
        @SuppressWarnings("unchecked")
        @Override public <T> T getProperty(String name, Class<T> type) { return (T) props.get(name); }
    }

    private static class DirectoryStub extends DirectoryServiceGrpc.DirectoryServiceImplBase {
        public void resolveCatalog(
            ResolveCatalogRequest request,
            StreamObserver<ResolveCatalogResponse> responseObserver) {
            ResourceId catalogId = ResourceId.newBuilder()
                .setId("catalog-id")
                .setAccountId("account")
                .setKind(ResourceKind.RK_CATALOG)
                .build();
            responseObserver.onNext(ResolveCatalogResponse.newBuilder().setResourceId(catalogId).build());
            responseObserver.onCompleted();
        }
    }

    private static class LifecycleStub extends QueryServiceGrpc.QueryServiceImplBase {
        volatile BeginQueryRequest lastBegin;
        @Override
        public void beginQuery(
            BeginQueryRequest request,
            StreamObserver<BeginQueryResponse> responseObserver) {
            this.lastBegin = request;
            QueryDescriptor d = QueryDescriptor.newBuilder().setQueryId("q").build();
            responseObserver.onNext(BeginQueryResponse.newBuilder().setQuery(d).build());
            responseObserver.onCompleted();
        }
    }

    private static class SchemaStub extends QuerySchemaServiceGrpc.QuerySchemaServiceImplBase {
        volatile DescribeInputsRequest lastDescribe;
        @Override
        public void describeInputs(
            DescribeInputsRequest request,
            StreamObserver<DescribeInputsResponse> responseObserver) {
            this.lastDescribe = request;
            responseObserver.onNext(DescribeInputsResponse.newBuilder().build());
            responseObserver.onCompleted();
        }
    }

    private static class ScanStub extends QueryScanServiceGrpc.QueryScanServiceImplBase {
        volatile FetchScanBundleRequest lastFetch;
        volatile String dataFileFormat = "PARQUET";
        volatile String partitionDataJson;
        @Override
        public void fetchScanBundle(
            FetchScanBundleRequest request,
            StreamObserver<FetchScanBundleResponse> responseObserver) {
            this.lastFetch = request;
            ScanFile.Builder f =
                ScanFile.newBuilder()
                    .setFilePath("/tmp/file")
                    .setFileFormat(dataFileFormat)
                    .setFileSizeInBytes(10)
                    .setRecordCount(1);
            if (partitionDataJson != null)
                f.setPartitionDataJson(partitionDataJson);
            ScanBundle bundle =
                ScanBundle.newBuilder().addDataFiles(f.build()).build();
            responseObserver.onNext(
                FetchScanBundleResponse.newBuilder()
                    .setBundle(bundle)
                    .build());
            responseObserver.onCompleted();
        }
    }

    private static void assumeProcessAccessible(Throwable t) {
        Throwable c = t;
        while (c != null) {
            String msg = c.getMessage();
            if (msg != null &&
                msg.contains("io.smallrye.common.os.Process")) {
                Assumptions.assumeTrue(false);
            }
            c = c.getCause();
        }
    }
}
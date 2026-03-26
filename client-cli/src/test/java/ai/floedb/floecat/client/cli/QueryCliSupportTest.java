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

package ai.floedb.floecat.client.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.floecat.catalog.rpc.LookupCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.execution.rpc.ScanBundle;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.BeginQueryResponse;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsResponse;
import ai.floedb.floecat.query.rpc.EndQueryRequest;
import ai.floedb.floecat.query.rpc.EndQueryResponse;
import ai.floedb.floecat.query.rpc.FetchScanBundleRequest;
import ai.floedb.floecat.query.rpc.FetchScanBundleResponse;
import ai.floedb.floecat.query.rpc.GetQueryRequest;
import ai.floedb.floecat.query.rpc.GetQueryResponse;
import ai.floedb.floecat.query.rpc.QueryDescriptor;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.query.rpc.RenewQueryRequest;
import ai.floedb.floecat.query.rpc.RenewQueryResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class QueryCliSupportTest {

  private static final String ACCOUNT_ID = "account-uuid-1";
  private static final String CATALOG_NAME = "my-catalog";
  private static final String QUERY_ID = "query-uuid-42";

  // --- empty args ---

  @Test
  void queryEmptyArgsPrintsUsage() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      QueryCliSupport.handle(
          "query",
          List.of(),
          new PrintStream(buf),
          h.queriesStub,
          h.queryScanStub,
          h.querySchemaStub,
          h.directoryStub,
          () -> CATALOG_NAME,
          () -> ACCOUNT_ID);
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  @Test
  void queryUnknownSubcommandPrintsUsage() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      QueryCliSupport.handle(
          "query",
          List.of("frobnicate"),
          new PrintStream(buf),
          h.queriesStub,
          h.queryScanStub,
          h.querySchemaStub,
          h.directoryStub,
          () -> CATALOG_NAME,
          () -> ACCOUNT_ID);
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- query renew ---

  @Test
  void queryRenewCallsServiceAndPrintsQueryId() throws Exception {
    try (Harness h = new Harness()) {
      h.queryService.renewResponseQueryId = QUERY_ID;

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      QueryCliSupport.handle(
          "query",
          List.of("renew", QUERY_ID),
          new PrintStream(buf),
          h.queriesStub,
          h.queryScanStub,
          h.querySchemaStub,
          h.directoryStub,
          () -> CATALOG_NAME,
          () -> ACCOUNT_ID);

      assertEquals(1, h.queryService.renewQueryCalls.get());
      assertEquals(QUERY_ID, h.queryService.lastRenewRequest.getQueryId());
      assertTrue(buf.toString().contains(QUERY_ID));
    }
  }

  @Test
  void queryRenewWithTtlSetsField() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      QueryCliSupport.handle(
          "query",
          List.of("renew", QUERY_ID, "--ttl", "120"),
          new PrintStream(buf),
          h.queriesStub,
          h.queryScanStub,
          h.querySchemaStub,
          h.directoryStub,
          () -> CATALOG_NAME,
          () -> ACCOUNT_ID);

      assertEquals(120, h.queryService.lastRenewRequest.getTtlSeconds());
    }
  }

  @Test
  void queryRenewPrintsUsageWhenMissingArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      QueryCliSupport.handle(
          "query",
          List.of("renew"),
          new PrintStream(buf),
          h.queriesStub,
          h.queryScanStub,
          h.querySchemaStub,
          h.directoryStub,
          () -> CATALOG_NAME,
          () -> ACCOUNT_ID);
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- query end ---

  @Test
  void queryEndCallsServiceAndPrintsQueryId() throws Exception {
    try (Harness h = new Harness()) {
      h.queryService.endResponseQueryId = QUERY_ID;

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      QueryCliSupport.handle(
          "query",
          List.of("end", QUERY_ID),
          new PrintStream(buf),
          h.queriesStub,
          h.queryScanStub,
          h.querySchemaStub,
          h.directoryStub,
          () -> CATALOG_NAME,
          () -> ACCOUNT_ID);

      assertEquals(1, h.queryService.endQueryCalls.get());
      assertEquals(QUERY_ID, h.queryService.lastEndRequest.getQueryId());
      assertTrue(buf.toString().contains(QUERY_ID));
    }
  }

  @Test
  void queryEndWithCommitFlagSetsField() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      QueryCliSupport.handle(
          "query",
          List.of("end", QUERY_ID, "--commit"),
          new PrintStream(buf),
          h.queriesStub,
          h.queryScanStub,
          h.querySchemaStub,
          h.directoryStub,
          () -> CATALOG_NAME,
          () -> ACCOUNT_ID);

      assertTrue(h.queryService.lastEndRequest.getCommit());
    }
  }

  @Test
  void queryEndPrintsUsageWhenMissingArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      QueryCliSupport.handle(
          "query",
          List.of("end"),
          new PrintStream(buf),
          h.queriesStub,
          h.queryScanStub,
          h.querySchemaStub,
          h.directoryStub,
          () -> CATALOG_NAME,
          () -> ACCOUNT_ID);
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- query get ---

  @Test
  void queryGetCallsServiceAndPrintsDescriptor() throws Exception {
    try (Harness h = new Harness()) {
      h.queryService.queryDescriptorToReturn =
          QueryDescriptor.newBuilder().setQueryId(QUERY_ID).build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      QueryCliSupport.handle(
          "query",
          List.of("get", QUERY_ID),
          new PrintStream(buf),
          h.queriesStub,
          h.queryScanStub,
          h.querySchemaStub,
          h.directoryStub,
          () -> CATALOG_NAME,
          () -> ACCOUNT_ID);

      assertEquals(1, h.queryService.getQueryCalls.get());
      assertEquals(QUERY_ID, h.queryService.lastGetRequest.getQueryId());
      assertTrue(buf.toString().contains("query id: " + QUERY_ID));
    }
  }

  @Test
  void queryGetPrintsUsageWhenMissingArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      QueryCliSupport.handle(
          "query",
          List.of("get"),
          new PrintStream(buf),
          h.queriesStub,
          h.queryScanStub,
          h.querySchemaStub,
          h.directoryStub,
          () -> CATALOG_NAME,
          () -> ACCOUNT_ID);
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- query fetch-scan ---

  @Test
  void queryFetchScanCallsServiceAndPrintsOutput() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      QueryCliSupport.handle(
          "query",
          List.of("fetch-scan", QUERY_ID, "table-uuid-99"),
          new PrintStream(buf),
          h.queriesStub,
          h.queryScanStub,
          h.querySchemaStub,
          h.directoryStub,
          () -> CATALOG_NAME,
          () -> ACCOUNT_ID);

      assertEquals(1, h.queryScanService.fetchScanBundleCalls.get());
      assertEquals(QUERY_ID, h.queryScanService.lastFetchScanRequest.getQueryId());
      assertTrue(buf.toString().contains("query id: " + QUERY_ID));
    }
  }

  @Test
  void queryFetchScanPrintsUsageWhenMissingArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      QueryCliSupport.handle(
          "query",
          List.of("fetch-scan", QUERY_ID),
          new PrintStream(buf),
          h.queriesStub,
          h.queryScanStub,
          h.querySchemaStub,
          h.directoryStub,
          () -> CATALOG_NAME,
          () -> ACCOUNT_ID);
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- query begin ---

  @Test
  void queryBeginWithoutCatalogPrintsError() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      QueryCliSupport.handle(
          "query",
          List.of("begin", "table", "cat.ns.tbl"),
          new PrintStream(buf),
          h.queriesStub,
          h.queryScanStub,
          h.querySchemaStub,
          h.directoryStub,
          () -> "",
          () -> ACCOUNT_ID);
      assertTrue(buf.toString().contains("no default catalog set"));
    }
  }

  @Test
  void queryBeginWithEmptyInputsPrintsUsage() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      QueryCliSupport.handle(
          "query",
          List.of("begin"),
          new PrintStream(buf),
          h.queriesStub,
          h.queryScanStub,
          h.querySchemaStub,
          h.directoryStub,
          () -> CATALOG_NAME,
          () -> ACCOUNT_ID);
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  @Test
  void queryBeginWithTableInputCallsService() throws Exception {
    try (Harness h = new Harness()) {
      h.queryService.beginResponseQueryId = QUERY_ID;

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      QueryCliSupport.handle(
          "query",
          List.of("begin", "table", "cat.ns.tbl"),
          new PrintStream(buf),
          h.queriesStub,
          h.queryScanStub,
          h.querySchemaStub,
          h.directoryStub,
          () -> CATALOG_NAME,
          () -> ACCOUNT_ID);

      assertEquals(1, h.queryService.beginQueryCalls.get());
      assertEquals(1, h.querySchemaService.describeInputsCalls.get());
      assertTrue(buf.toString().contains(QUERY_ID));
    }
  }

  // --- test infrastructure ---

  private static final class Harness implements AutoCloseable {
    final Server server;
    final ManagedChannel channel;
    final CapturingQueryService queryService;
    final CapturingQueryScanService queryScanService;
    final CapturingQuerySchemaService querySchemaService;
    final CapturingDirectoryService directoryService;
    final QueryServiceGrpc.QueryServiceBlockingStub queriesStub;
    final QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScanStub;
    final QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub querySchemaStub;
    final DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub;

    Harness() throws Exception {
      String serverName = InProcessServerBuilder.generateName();
      this.queryService = new CapturingQueryService();
      this.queryScanService = new CapturingQueryScanService();
      this.querySchemaService = new CapturingQuerySchemaService();
      this.directoryService = new CapturingDirectoryService();
      this.server =
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(queryService)
              .addService(queryScanService)
              .addService(querySchemaService)
              .addService(directoryService)
              .build()
              .start();
      this.channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
      this.queriesStub = QueryServiceGrpc.newBlockingStub(channel);
      this.queryScanStub = QueryScanServiceGrpc.newBlockingStub(channel);
      this.querySchemaStub = QuerySchemaServiceGrpc.newBlockingStub(channel);
      this.directoryStub = DirectoryServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void close() throws Exception {
      channel.shutdownNow();
      server.shutdownNow();
    }
  }

  private static final class CapturingQueryService extends QueryServiceGrpc.QueryServiceImplBase {

    final AtomicInteger beginQueryCalls = new AtomicInteger();
    final AtomicInteger renewQueryCalls = new AtomicInteger();
    final AtomicInteger endQueryCalls = new AtomicInteger();
    final AtomicInteger getQueryCalls = new AtomicInteger();
    BeginQueryRequest lastBeginRequest;
    RenewQueryRequest lastRenewRequest;
    EndQueryRequest lastEndRequest;
    GetQueryRequest lastGetRequest;
    String beginResponseQueryId = "begin-query-id";
    String renewResponseQueryId = "renew-query-id";
    String endResponseQueryId = "end-query-id";
    QueryDescriptor queryDescriptorToReturn = QueryDescriptor.getDefaultInstance();

    @Override
    public void beginQuery(
        BeginQueryRequest request, StreamObserver<BeginQueryResponse> responseObserver) {
      beginQueryCalls.incrementAndGet();
      lastBeginRequest = request;
      responseObserver.onNext(
          BeginQueryResponse.newBuilder()
              .setQuery(QueryDescriptor.newBuilder().setQueryId(beginResponseQueryId).build())
              .build());
      responseObserver.onCompleted();
    }

    @Override
    public void renewQuery(
        RenewQueryRequest request, StreamObserver<RenewQueryResponse> responseObserver) {
      renewQueryCalls.incrementAndGet();
      lastRenewRequest = request;
      responseObserver.onNext(
          RenewQueryResponse.newBuilder().setQueryId(renewResponseQueryId).build());
      responseObserver.onCompleted();
    }

    @Override
    public void endQuery(
        EndQueryRequest request, StreamObserver<EndQueryResponse> responseObserver) {
      endQueryCalls.incrementAndGet();
      lastEndRequest = request;
      responseObserver.onNext(EndQueryResponse.newBuilder().setQueryId(endResponseQueryId).build());
      responseObserver.onCompleted();
    }

    @Override
    public void getQuery(
        GetQueryRequest request, StreamObserver<GetQueryResponse> responseObserver) {
      getQueryCalls.incrementAndGet();
      lastGetRequest = request;
      responseObserver.onNext(
          GetQueryResponse.newBuilder().setQuery(queryDescriptorToReturn).build());
      responseObserver.onCompleted();
    }
  }

  private static final class CapturingQueryScanService
      extends QueryScanServiceGrpc.QueryScanServiceImplBase {

    final AtomicInteger fetchScanBundleCalls = new AtomicInteger();
    FetchScanBundleRequest lastFetchScanRequest;

    @Override
    public void fetchScanBundle(
        FetchScanBundleRequest request, StreamObserver<FetchScanBundleResponse> responseObserver) {
      fetchScanBundleCalls.incrementAndGet();
      lastFetchScanRequest = request;
      responseObserver.onNext(
          FetchScanBundleResponse.newBuilder().setBundle(ScanBundle.getDefaultInstance()).build());
      responseObserver.onCompleted();
    }
  }

  private static final class CapturingQuerySchemaService
      extends QuerySchemaServiceGrpc.QuerySchemaServiceImplBase {

    final AtomicInteger describeInputsCalls = new AtomicInteger();

    @Override
    public void describeInputs(
        DescribeInputsRequest request, StreamObserver<DescribeInputsResponse> responseObserver) {
      describeInputsCalls.incrementAndGet();
      responseObserver.onNext(DescribeInputsResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static final class CapturingDirectoryService
      extends DirectoryServiceGrpc.DirectoryServiceImplBase {

    @Override
    public void resolveCatalog(
        ResolveCatalogRequest request, StreamObserver<ResolveCatalogResponse> responseObserver) {
      responseObserver.onNext(
          ResolveCatalogResponse.newBuilder()
              .setResourceId(ResourceId.newBuilder().setId("cat-resolved-id").build())
              .build());
      responseObserver.onCompleted();
    }

    @Override
    public void lookupCatalog(
        LookupCatalogRequest request, StreamObserver<LookupCatalogResponse> responseObserver) {
      responseObserver.onNext(LookupCatalogResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }
}

package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.context.impl.TestEngineVersionCaptureInterceptor;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class PropagationIT {
  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @Inject QueryContextStore queryStore;

  private static final Metadata.Key<byte[]> PRINCIPAL_BIN =
      Metadata.Key.of("x-principal-bin", Metadata.BINARY_BYTE_MARSHALLER);
  private static final Metadata.Key<String> QUERY_ID =
      Metadata.Key.of("x-query-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> ENGINE_VERSION =
      Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORR =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);

  private static PrincipalContext pc() {
    ResourceId accountId = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT);
    return PrincipalContext.newBuilder()
        .setAccountId(accountId.getId())
        .setSubject("it-user")
        .addPermissions("catalog.read")
        .build();
  }

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
    TestEngineVersionCaptureInterceptor.reset();
  }

  @Test
  void correlationIdEchoed() {
    ResourceId accountId = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT);
    String corr = "it-corr-" + UUID.randomUUID();

    Metadata m = new Metadata();
    m.put(PRINCIPAL_BIN, pc().toByteArray());
    m.put(CORR, corr);

    RespHeadersCaptureInterceptor capture = new RespHeadersCaptureInterceptor();
    var client =
        catalog
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(m))
            .withInterceptors(capture);

    client.listCatalogs(ListCatalogsRequest.getDefaultInstance());

    String echoed =
        Optional.ofNullable(capture.responseHeaders.get())
            .map(h -> h.get(CORR))
            .orElseGet(
                () ->
                    Optional.ofNullable(capture.responseTrailers.get())
                        .map(t -> t.get(CORR))
                        .orElse(null));

    assertEquals(corr, echoed, "server should echo x-correlation-id");

    var rid =
        ResourceId.newBuilder()
            .setAccountId(accountId.getId())
            .setKind(ResourceKind.RK_CATALOG)
            .setId("00000000-0000-0000-0000-000000000000")
            .build();

    RespHeadersCaptureInterceptor captureErr = new RespHeadersCaptureInterceptor();
    var errClient =
        catalog
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(m))
            .withInterceptors(captureErr);

    assertThrows(
        StatusRuntimeException.class,
        () -> errClient.getCatalog(GetCatalogRequest.newBuilder().setCatalogId(rid).build()));

    String echoedOnErr =
        Optional.ofNullable(captureErr.responseTrailers.get()).map(t -> t.get(CORR)).orElse(null);
    assertEquals(corr, echoedOnErr, "server should echo x-correlation-id in trailers on error");
  }

  @Test
  void loadPrincipalFromStore() {
    String queryId = "query-" + UUID.randomUUID();
    var seededPc = pc().toBuilder().setQueryId(queryId).build();

    queryStore.put(
        QueryContext.newActive(
            queryId,
            seededPc,
            null,
            null,
            null,
            null,
            60_000L,
            1L,
            ResourceId.newBuilder().setId("cat-it").build()));

    String corr = "it-corr-" + UUID.randomUUID();

    Metadata m = new Metadata();
    m.put(QUERY_ID, queryId);
    m.put(CORR, corr);

    RespHeadersCaptureInterceptor capture = new RespHeadersCaptureInterceptor();
    var client =
        catalog
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(m))
            .withInterceptors(capture);

    client.listCatalogs(ListCatalogsRequest.getDefaultInstance());

    String echoed =
        Optional.ofNullable(capture.responseHeaders.get())
            .map(h -> h.get(CORR))
            .orElseGet(
                () ->
                    Optional.ofNullable(capture.responseTrailers.get())
                        .map(t -> t.get(CORR))
                        .orElse(null));
    assertEquals(corr, echoed, "server should echo x-correlation-id");
  }

  /** Verifies engine version headers make it through the inbound server stack. */
  @Test
  void engineVersionPropagatesToOutboundCalls() {
    String corr = "it-corr-" + UUID.randomUUID();

    Metadata principalMeta = new Metadata();
    principalMeta.put(PRINCIPAL_BIN, pc().toByteArray());
    principalMeta.put(CORR, corr);

    Metadata engineMeta = new Metadata();
    engineMeta.put(ENGINE_VERSION, "it-engine");

    catalog
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(engineMeta))
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(principalMeta))
        .listCatalogs(ListCatalogsRequest.getDefaultInstance());

    assertEquals("it-engine", TestEngineVersionCaptureInterceptor.captured());
  }

  private static final class RespHeadersCaptureInterceptor implements ClientInterceptor {
    final AtomicReference<Metadata> responseHeaders = new AtomicReference<>();
    final AtomicReference<Metadata> responseTrailers = new AtomicReference<>();

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
      return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headersIn) {
          super.start(
              new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(
                  responseListener) {
                @Override
                public void onHeaders(Metadata h) {
                  responseHeaders.set(h);
                  super.onHeaders(h);
                }

                @Override
                public void onClose(Status status, Metadata t) {
                  responseTrailers.set(t);
                  super.onClose(status, t);
                }
              },
              headersIn);
        }
      };
    }
  }
}

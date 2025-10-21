package ai.floedb.metacat.service.it;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.planning.PlanContextStore;
import ai.floedb.metacat.service.planning.impl.PlanContext;

@QuarkusTest
class PropagationIT {

  @GrpcClient("resource-access")
  ResourceAccessGrpc.ResourceAccessBlockingStub resourceAccess;

  @Inject PlanContextStore planStore;

  private static final Metadata.Key<byte[]> PRINCIPAL_BIN =
      Metadata.Key.of("x-principal-bin", Metadata.BINARY_BYTE_MARSHALLER);
  private static final Metadata.Key<String> PLAN_ID =
      Metadata.Key.of("x-plan-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORR =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);

  private static PrincipalContext pc(String tenant) {
    return PrincipalContext.newBuilder()
        .setTenantId(tenant).setSubject("it-user")
        .addPermissions("catalog.read").build();
  }

  @Test
  void corr_is_echoed_and_principal_is_parsed_on_success() {
    String corr = "it-corr-" + UUID.randomUUID();

    Metadata m = new Metadata();
    m.put(PRINCIPAL_BIN, pc("t-0001").toByteArray());
    m.put(CORR, corr);

    RespHeadersCaptureInterceptor capture = new RespHeadersCaptureInterceptor();
    var client = resourceAccess
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(m))
        .withInterceptors(capture);

    client.listCatalogs(ListCatalogsRequest.getDefaultInstance());

    String echoed =
        Optional.ofNullable(capture.responseHeaders.get())
            .map(h -> h.get(CORR))
            .orElseGet(() -> Optional.ofNullable(capture.responseTrailers.get())
                .map(t -> t.get(CORR)).orElse(null));

    assertEquals(corr, echoed, "server should echo x-correlation-id");

    var rid = ResourceId.newBuilder()
        .setTenantId("t-0001")
        .setKind(ResourceKind.RK_CATALOG)
        .setId("00000000-0000-0000-0000-000000000000")
        .build();

    RespHeadersCaptureInterceptor captureErr = new RespHeadersCaptureInterceptor();
    var errClient = resourceAccess
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(m))
        .withInterceptors(captureErr);

    assertThrows(
        StatusRuntimeException.class,
        () -> errClient.getCatalog(GetCatalogRequest.newBuilder().setCatalogId(rid).build()));

    String echoedOnErr =
        Optional.ofNullable(captureErr.responseTrailers.get())
            .map(t -> t.get(CORR))
            .orElse(null);
    assertEquals(corr, echoedOnErr, "server should echo x-correlation-id in trailers on error");
  }

  @Test
  void principal_loads_from_plan_store_when_header_missing() {
    String planId = "plan-" + UUID.randomUUID();
    var seededPc = pc("t-4242").toBuilder().setPlanId(planId).build();

    planStore.put(PlanContext.newActive(
        planId,
        "t-4242",
        seededPc,
        null, null,
        60_000L,
        1L
    ));

    String corr = "it-corr-" + UUID.randomUUID();

    Metadata m = new Metadata();
    m.put(PLAN_ID, planId);
    m.put(CORR, corr);

    RespHeadersCaptureInterceptor capture = new RespHeadersCaptureInterceptor();
    var client = resourceAccess
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(m))
        .withInterceptors(capture);

    client.listCatalogs(ListCatalogsRequest.getDefaultInstance());

    String echoed =
        Optional.ofNullable(capture.responseHeaders.get())
            .map(h -> h.get(CORR))
            .orElseGet(() -> Optional.ofNullable(capture.responseTrailers.get())
                .map(t -> t.get(CORR)).orElse(null));
    assertEquals(corr, echoed, "server should echo x-correlation-id");
  }

  static final class RespHeadersCaptureInterceptor implements ClientInterceptor {
    final AtomicReference<Metadata> responseHeaders = new AtomicReference<>();
    final AtomicReference<Metadata> responseTrailers = new AtomicReference<>();

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
      return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
        @Override public void start(Listener<RespT> responseListener, Metadata headersIn) {
          super.start(new ForwardingClientCallListener
              .SimpleForwardingClientCallListener<>(responseListener) {
            @Override public void onHeaders(Metadata h) {
              responseHeaders.set(h);
              super.onHeaders(h);
            }
            
            @Override public void onClose(Status status, Metadata t) {
              responseTrailers.set(t); super.onClose(status, t);
            }
          }, headersIn);
        }
      };
    }
  }
}

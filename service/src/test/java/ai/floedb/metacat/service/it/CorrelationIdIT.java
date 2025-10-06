package ai.floedb.metacat.service.it;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;

@QuarkusTest
class CorrelationIdIT {
  @GrpcClient("catalog")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  private static final Metadata.Key<String> CORR_KEY =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<byte[]> PRINC_BIN =
      Metadata.Key.of("x-principal-bin", Metadata.BINARY_BYTE_MARSHALLER);

  record StubWithCapture(CatalogServiceGrpc.CatalogServiceBlockingStub stub,
                         CaptureHeadersInterceptor capture) {}

  static final class CaptureHeadersInterceptor implements ClientInterceptor {
    final AtomicReference<Metadata> headers = new AtomicReference<>();
    final AtomicReference<Metadata> trailers = new AtomicReference<>();

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
      return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
        @Override public void start(Listener<RespT> responseListener, Metadata headersIn) {
          super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
            @Override public void onHeaders(Metadata h) {
              headers.set(h);
              super.onHeaders(h);
            }
            @Override public void onClose(Status status, Metadata t) {
              trailers.set(t);
              super.onClose(status, t);
            }
          }, headersIn);
        }
      };
    }
  }

  private StubWithCapture stubWith(String corrId, String tenantId) {
    var pc = ai.floedb.metacat.common.rpc.PrincipalContext.newBuilder()
        .setTenantId(tenantId).setSubject("it-user").addPermissions("catalog.read").build();

    Metadata hdrs = new Metadata();
    hdrs.put(CORR_KEY, corrId);
    hdrs.put(PRINC_BIN, pc.toByteArray());

    var attach = MetadataUtils.newAttachHeadersInterceptor(hdrs);
    var capture = new CaptureHeadersInterceptor();

    var stub = catalog.withInterceptors(attach, capture);
    return new StubWithCapture(stub, capture);
  }

  @Test
  void correlationId_in_headers_on_success() {
    var sc = stubWith("it-corr-success-123", "t-corr-0001");
    var page1 = sc.stub().listCatalogs(ListCatalogsRequest.getDefaultInstance());
    var headers = sc.capture().headers.get();
    Assertions.assertEquals("it-corr-success-123", headers.get(CORR_KEY));
  }

  @Test
  void correlationId_in_trailers_on_error() {
    var sc = stubWith("it-corr-error-456", "t-corr-0001");
    var rid = ResourceId.newBuilder()
        .setTenantId("t-corr-0001").setId("00000000-0000-0000-0000-000000000000")
        .setKind(ResourceKind.RK_CATALOG).build();

    var ex = Assertions.assertThrows(StatusRuntimeException.class,
        () -> sc.stub().getCatalog(GetCatalogRequest.newBuilder().setResourceId(rid).build()));

    var trailers = Optional.ofNullable(sc.capture().trailers.get())
        .orElse(Status.trailersFromThrowable(ex));

    Assertions.assertEquals("it-corr-error-456", trailers.get(CORR_KEY));
  }
}
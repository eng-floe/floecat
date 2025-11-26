package ai.floedb.metacat.service.catalog.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.metacat.catalog.rpc.BuiltinCatalogServiceGrpc;
import ai.floedb.metacat.catalog.rpc.GetBuiltinCatalogRequest;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
class BuiltinCatalogServiceIT {

  private static final Metadata.Key<String> ENGINE_HEADER =
      Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);

  @GrpcClient("metacat")
  BuiltinCatalogServiceGrpc.BuiltinCatalogServiceBlockingStub builtins;

  @Test
  void returnsCatalogWhenVersionProvided() {
    var stub = withEngineVersion("demo-pg-builtins");
    var resp = stub.getBuiltinCatalog(GetBuiltinCatalogRequest.newBuilder().build());
    assertThat(resp.hasCatalog()).isTrue();
    assertThat(resp.getCatalog().getVersion()).isEqualTo("demo-pg-builtins");
  }

  @Test
  void currentVersionShortCircuits() {
    var stub = withEngineVersion("demo-pg-builtins");
    var resp =
        stub.getBuiltinCatalog(
            GetBuiltinCatalogRequest.newBuilder().setCurrentVersion("demo-pg-builtins").build());
    assertThat(resp.hasCatalog()).isFalse();
  }

  @Test
  void missingHeaderFails() {
    assertThatThrownBy(
            () -> builtins.getBuiltinCatalog(GetBuiltinCatalogRequest.newBuilder().build()))
        .isInstanceOf(StatusRuntimeException.class);
  }

  private BuiltinCatalogServiceGrpc.BuiltinCatalogServiceBlockingStub withEngineVersion(
      String engineVersion) {
    var metadata = new Metadata();
    metadata.put(ENGINE_HEADER, engineVersion);
    return MetadataUtils.attachHeaders(builtins, metadata);
  }
}

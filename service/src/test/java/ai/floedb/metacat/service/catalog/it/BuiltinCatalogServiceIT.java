package ai.floedb.metacat.service.catalog.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.metacat.catalog.rpc.BuiltinCatalogServiceGrpc;
import ai.floedb.metacat.catalog.rpc.GetBuiltinCatalogRequest;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import io.grpc.Status;

@QuarkusTest
class BuiltinCatalogServiceIT {

  private static final Metadata.Key<String> ENGINE_HEADER =
      Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);

  @GrpcClient("metacat")
  BuiltinCatalogServiceGrpc.BuiltinCatalogServiceBlockingStub builtins;

  /** Full RPC fetch should return the builtin catalog and all expected entries. */
  @Test
  void returnsCatalogWhenVersionProvided() {
    var stub = withEngineVersion("demo-pg-builtins");
    var resp = stub.getBuiltinCatalog(GetBuiltinCatalogRequest.newBuilder().build());
    assertThat(resp.hasCatalog()).isTrue();
    var catalog = resp.getCatalog();
    assertThat(catalog.getVersion()).isEqualTo("demo-pg-builtins");
    assertThat(catalog.getFunctionsCount()).isEqualTo(7);
    assertThat(catalog.getFunctionsList())
        .extracting(f -> f.getName())
        .contains("pg_catalog.int4_add", "pg_catalog.text_upper");
    assertThat(catalog.getOperatorsList())
        .extracting(op -> op.getFunctionName())
        .contains("pg_catalog.int4_add");
    assertThat(catalog.getTypesList())
        .extracting(t -> t.getName())
        .contains("pg_catalog._int4");
    assertThat(catalog.getCastsList())
        .extracting(c -> c.getSourceType())
        .contains("pg_catalog.text");
    assertThat(catalog.getCollationsList()).hasSize(1);
    assertThat(catalog.getAggregatesList())
        .extracting(a -> a.getName())
        .contains("pg_catalog.sum");
  }

  /** If the caller already has the latest version, the RPC should return an empty payload. */
  @Test
  void currentVersionShortCircuits() {
    var stub = withEngineVersion("demo-pg-builtins");
    var resp =
        stub.getBuiltinCatalog(
            GetBuiltinCatalogRequest.newBuilder().setCurrentVersion("demo-pg-builtins").build());
    assertThat(resp.hasCatalog()).isFalse();
  }

  /** Requests must include x-engine-version; missing headers should fail fast. */
  @Test
  void missingHeaderFails() {
    assertThatThrownBy(
            () -> builtins.getBuiltinCatalog(GetBuiltinCatalogRequest.newBuilder().build()))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
  }

  /** Unknown engine versions map to NOT_FOUND to keep behavior deterministic. */
  @Test
  void notFoundEngineVersion() {
    var stub = withEngineVersion("does-not-exist");
    assertThatThrownBy(() -> stub.getBuiltinCatalog(GetBuiltinCatalogRequest.newBuilder().build()))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
  }

  private BuiltinCatalogServiceGrpc.BuiltinCatalogServiceBlockingStub withEngineVersion(
      String engineVersion) {
    var metadata = new Metadata();
    metadata.put(ENGINE_HEADER, engineVersion);
    return builtins.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }
}

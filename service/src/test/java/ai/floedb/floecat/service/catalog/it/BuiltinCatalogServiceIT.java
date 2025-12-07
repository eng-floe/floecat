package ai.floedb.floecat.service.catalog.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.BuiltinCatalogServiceGrpc;
import ai.floedb.floecat.query.rpc.GetBuiltinCatalogRequest;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
class BuiltinCatalogServiceIT {

  private static final Metadata.Key<String> ENGINE_VERSION_HEADER =
      Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);

  private static final Metadata.Key<String> ENGINE_KIND_HEADER =
      Metadata.Key.of("x-engine-kind", Metadata.ASCII_STRING_MARSHALLER);

  @GrpcClient("floecat")
  BuiltinCatalogServiceGrpc.BuiltinCatalogServiceBlockingStub builtins;

  @Test
  void returnsCatalogWhenVersionProvided() {
    var stub = withEngineHeaders("floe-demo", "16.0");

    var resp = stub.getBuiltinCatalog(GetBuiltinCatalogRequest.getDefaultInstance());

    assertThat(resp.hasRegistry()).isTrue();

    var reg = resp.getRegistry();
    assertThat(reg.getFunctionsCount()).isEqualTo(7);

    // FUNCTION NAMES
    assertThat(reg.getFunctionsList())
        .extracting(f -> fullName(f.getName()))
        .contains("pg_catalog.int4_add", "pg_catalog.text_upper");

    // OPERATORS – operator name is just the symbol
    assertThat(reg.getOperatorsList())
        .extracting(op -> fullName(op.getName()))
        .containsExactlyInAnyOrder("+", "||");

    // TYPES
    assertThat(reg.getTypesList())
        .extracting(t -> fullName(t.getName()))
        .contains("pg_catalog._int4");

    // CASTS
    assertThat(reg.getCastsList())
        .extracting(c -> fullName(c.getSourceType()))
        .contains("pg_catalog.text");

    // COLLATIONS
    assertThat(reg.getCollationsList()).hasSize(1);

    // AGGREGATES
    assertThat(reg.getAggregatesList())
        .extracting(a -> fullName(a.getName()))
        .contains("pg_catalog.sum");
  }

  @Test
  void missingHeaderFails() {
    assertThatThrownBy(
            () -> builtins.getBuiltinCatalog(GetBuiltinCatalogRequest.newBuilder().build()))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
  }

  @Test
  void unknownEngineVersionReturnsOnlyRuleFreeObjects() {
    var stub = withEngineHeaders("floe-demo", "does-not-exist");

    var resp = stub.getBuiltinCatalog(GetBuiltinCatalogRequest.getDefaultInstance());
    assertThat(resp.hasRegistry()).isTrue();

    var names =
        resp.getRegistry().getFunctionsList().stream().map(f -> fullName(f.getName())).toList();

    assertThat(names)
        .containsExactlyInAnyOrder(
            "pg_catalog.text_length", "pg_catalog.int4_add", "pg_catalog.text_concat");

    assertThat(names)
        .doesNotContain(
            "pg_catalog.int4_abs", "pg_catalog.sum_int4_state", "pg_catalog.sum_int4_final");
  }

  @Test
  void missingEngineKindFails() {
    var metadata = new Metadata();
    metadata.put(ENGINE_VERSION_HEADER, "16.0");

    var stub = builtins.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

    assertThatThrownBy(() -> stub.getBuiltinCatalog(GetBuiltinCatalogRequest.getDefaultInstance()))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
  }

  private BuiltinCatalogServiceGrpc.BuiltinCatalogServiceBlockingStub withEngineHeaders(
      String engineKind, String engineVersion) {

    var metadata = new Metadata();
    metadata.put(ENGINE_KIND_HEADER, engineKind);
    metadata.put(ENGINE_VERSION_HEADER, engineVersion);

    return builtins.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }

  /** Build a fully qualified name from NameRef.path + NameRef.name (ignores catalog). */
  private static String fullName(NameRef ref) {
    if (ref == null) {
      return "";
    }
    var path = String.join(".", ref.getPathList());
    if (path.isEmpty()) {
      return ref.getName();
    }
    return path + "." + ref.getName();
  }
}

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

package ai.floedb.floecat.service.catalog.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.GetSystemObjectsRequest;
import ai.floedb.floecat.query.rpc.SystemObjectsServiceGrpc;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
class SystemObjectsServiceIT {

  private static final Metadata.Key<String> ENGINE_VERSION_HEADER =
      Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);

  private static final Metadata.Key<String> ENGINE_KIND_HEADER =
      Metadata.Key.of("x-engine-kind", Metadata.ASCII_STRING_MARSHALLER);

  @GrpcClient("floecat")
  SystemObjectsServiceGrpc.SystemObjectsServiceBlockingStub builtins;

  @Test
  void returnsCatalogWhenVersionProvided() {
    var stub = withEngineHeaders("floe-demo", "16.0");

    var resp = stub.getSystemObjects(GetSystemObjectsRequest.getDefaultInstance());

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
            () -> builtins.getSystemObjects(GetSystemObjectsRequest.newBuilder().build()))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
  }

  @Test
  void unknownEngineVersionReturnsOnlyRuleFreeObjects() {
    var stub = withEngineHeaders("floe-demo", "does-not-exist");

    var resp = stub.getSystemObjects(GetSystemObjectsRequest.getDefaultInstance());
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

    assertThatThrownBy(() -> stub.getSystemObjects(GetSystemObjectsRequest.getDefaultInstance()))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
  }

  private SystemObjectsServiceGrpc.SystemObjectsServiceBlockingStub withEngineHeaders(
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

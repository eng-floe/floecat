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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.view;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.api.metadata.ViewMetadataView;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.ViewRequests;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.services.view.ViewBackend;
import ai.floedb.floecat.gateway.iceberg.minimal.services.view.ViewMetadataService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ViewRegisterResourceTest {
  private final ViewBackend backend = Mockito.mock(ViewBackend.class);
  private final ObjectMapper mapper = new ObjectMapper();
  private final ViewMetadataService metadataService = metadataServiceWithMapper();
  private final MinimalGatewayConfig config = new TestConfig();
  private final TestViewRegisterResource resource =
      new TestViewRegisterResource(backend, metadataService, mapper, config);

  @Test
  void registerReturnsLoadResult() throws Exception {
    View created =
        View.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("view-1"))
            .setDisplayName("orders_view")
            .setSql("select 1")
            .setDialect("ansi")
            .putProperties("metadata-location", "floecat://views/db/orders_view/metadata.json")
            .build();
    when(backend.create(
            eq("foo"),
            eq(List.of("db")),
            eq("orders_view"),
            any(),
            any(),
            any(),
            any(),
            any(),
            eq("idem-1")))
        .thenReturn(created);

    ViewMetadataView metadata =
        new ViewMetadataView(
            "v1",
            1,
            "floecat://views/db/orders_view/metadata.json",
            0,
            List.of(
                new ViewMetadataView.ViewVersion(
                    0,
                    1L,
                    0,
                    Map.of(),
                    List.of(new ViewMetadataView.ViewRepresentation("sql", "select 1", "ansi")),
                    List.of("db"),
                    null)),
            List.of(new ViewMetadataView.ViewHistoryEntry(0, 1L)),
            List.of(new ViewMetadataView.SchemaSummary(0, "struct", List.of(), List.of())),
            Map.of());

    assertEquals(
        200,
        resource
            .register(
                "foo",
                "db",
                "idem-1",
                new ViewRequests.Register("orders_view", "memory://metadata.json"),
                mapper.writeValueAsString(metadata))
            .getStatus());
  }

  private ViewMetadataService metadataServiceWithMapper() {
    ViewMetadataService service = new ViewMetadataService();
    try {
      var field = ViewMetadataService.class.getDeclaredField("mapper");
      field.setAccessible(true);
      field.set(service, mapper);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
    return service;
  }

  private static final class TestViewRegisterResource extends ViewRegisterResource {
    private String payload;

    private TestViewRegisterResource(
        ViewBackend backend,
        ViewMetadataService metadataService,
        ObjectMapper mapper,
        MinimalGatewayConfig config) {
      super(backend, metadataService, mapper, config);
    }

    private jakarta.ws.rs.core.Response register(
        String prefix,
        String namespace,
        String idempotencyKey,
        ViewRequests.Register request,
        String payload)
        throws Exception {
      this.payload = payload;
      return super.register(prefix, namespace, idempotencyKey, request);
    }

    @Override
    protected ViewMetadataView loadMetadata(String metadataLocation) {
      try {
        return new ObjectMapper().readValue(payload, ViewMetadataView.class);
      } catch (Exception e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    }
  }

  private static final class TestConfig implements MinimalGatewayConfig {
    @Override
    public String upstreamTarget() {
      return "localhost:9100";
    }

    @Override
    public boolean upstreamPlaintext() {
      return true;
    }

    @Override
    public Optional<String> defaultAccountId() {
      return Optional.of("acct");
    }

    @Override
    public Optional<String> defaultAuthorization() {
      return Optional.empty();
    }

    @Override
    public Optional<String> defaultPrefix() {
      return Optional.of("examples");
    }

    @Override
    public Map<String, String> catalogMapping() {
      return Map.of("examples", "examples");
    }

    @Override
    public Duration idempotencyKeyLifetime() {
      return Duration.ofMinutes(30);
    }

    @Override
    public Duration planTaskTtl() {
      return Duration.ofMinutes(5);
    }

    @Override
    public int planTaskFilesPerTask() {
      return 128;
    }

    @Override
    public Optional<StorageCredentialConfig> storageCredential() {
      return Optional.empty();
    }

    @Override
    public Optional<String> metadataFileIo() {
      return Optional.empty();
    }

    @Override
    public Optional<String> metadataFileIoRoot() {
      return Optional.empty();
    }

    @Override
    public Optional<String> metadataS3Endpoint() {
      return Optional.empty();
    }

    @Override
    public boolean metadataS3PathStyleAccess() {
      return true;
    }

    @Override
    public Optional<String> metadataS3Region() {
      return Optional.empty();
    }

    @Override
    public Optional<String> metadataClientRegion() {
      return Optional.empty();
    }

    @Override
    public Optional<String> metadataS3AccessKeyId() {
      return Optional.empty();
    }

    @Override
    public Optional<String> metadataS3SecretAccessKey() {
      return Optional.empty();
    }

    @Override
    public Optional<String> defaultWarehousePath() {
      return Optional.empty();
    }

    @Override
    public Optional<DeltaCompatConfig> deltaCompat() {
      return Optional.empty();
    }

    @Override
    public boolean logRequestBodies() {
      return false;
    }

    @Override
    public int logRequestBodyMaxChars() {
      return 8192;
    }
  }
}

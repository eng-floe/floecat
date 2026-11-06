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
package ai.floedb.floecat.service.query.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.FlightEndpointRef;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode.StorageSystemTableNode;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Verifies that a missing Flight mapping cannot silently become a storage scan. */
class SystemExecutionResolverTest {
  private final SystemExecutionResolver resolver = new SystemExecutionResolver(null);

  @Test
  void missingKeyedEndpointIsAnError() {
    assertThatThrownBy(() -> resolver.resolve(table("sys://test", "missing-flight-test", null)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("missing-flight-test");
  }

  @Test
  void unkeyedFileStorageRemainsAStorageScan() {
    var execution = resolver.resolve(table("s3://bucket/table", "", null));
    assertThat(execution.flightEndpoint()).isNull();
    assertThat(execution.storagePath()).isEqualTo("s3://bucket/table");
  }

  @Test
  void explicitEndpointTakesPrecedenceOverTheKey() {
    var endpoint =
        FlightEndpointRef.newBuilder()
            .setHost("provider.example")
            .setPort(443)
            .setTls(true)
            .build();
    assertThat(
            resolver.resolve(table("sys://test", "missing-flight-test", endpoint)).flightEndpoint())
        .isEqualTo(endpoint);
  }

  private static StorageSystemTableNode table(String path, String key, FlightEndpointRef endpoint) {
    return new StorageSystemTableNode(
        ResourceId.getDefaultInstance(),
        1,
        "",
        "test",
        ResourceId.getDefaultInstance(),
        List.of(),
        Map.of(),
        Map.of(),
        path,
        key,
        endpoint);
  }
}

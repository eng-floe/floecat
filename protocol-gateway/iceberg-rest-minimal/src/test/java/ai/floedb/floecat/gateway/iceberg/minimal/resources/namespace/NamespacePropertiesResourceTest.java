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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.namespace;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.NamespacePropertiesResponseDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.NamespacePropertiesRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.services.namespace.NamespaceBackend;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class NamespacePropertiesResourceTest {
  private final NamespaceBackend backend = Mockito.mock(NamespaceBackend.class);
  private final NamespacePropertiesResource resource = new NamespacePropertiesResource(backend);

  @Test
  void updatePropertiesReturnsAppliedChanges() {
    when(backend.get("foo", List.of("analytics")))
        .thenReturn(
            Namespace.newBuilder()
                .setDisplayName("analytics")
                .putProperties("owner", "team-a")
                .putProperties("env", "dev")
                .build());
    when(backend.updateProperties(
            eq("foo"),
            eq(List.of("analytics")),
            eq(Map.of("owner", "team-a", "region", "eu-west-1")),
            eq("idem-1")))
        .thenReturn(
            Namespace.newBuilder()
                .setDisplayName("analytics")
                .putProperties("owner", "team-a")
                .putProperties("region", "eu-west-1")
                .build());

    NamespacePropertiesResponseDto response =
        (NamespacePropertiesResponseDto)
            resource
                .updateProperties(
                    "foo",
                    "analytics",
                    "idem-1",
                    new NamespacePropertiesRequest(
                        List.of("env", "missing"), Map.of("region", "eu-west-1")))
                .getEntity();

    assertEquals(List.of("region"), response.updated());
    assertEquals(List.of("env"), response.removed());
    assertEquals(List.of("missing"), response.missing());
  }

  @Test
  void updatePropertiesRejectsConflictingKeys() {
    assertEquals(
        422,
        resource
            .updateProperties(
                "foo",
                "analytics",
                "idem-1",
                new NamespacePropertiesRequest(List.of("env"), Map.of("env", "prod")))
            .getStatus());
  }
}

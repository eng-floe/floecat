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

package ai.floedb.floecat.gateway.iceberg.rest.resources.table;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.CatalogRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.ResourceResolver;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitTrafficLogger;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCommitService;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TableResourceTest {
  @Test
  void commitLogsRequestAndResponse() {
    TableResource resource = new TableResource();
    resource.resourceResolver = mock(ResourceResolver.class);
    resource.tableCommitService = mock(TableCommitService.class);
    resource.commitTrafficLogger = mock(CommitTrafficLogger.class);
    resource.tableSupport = mock(TableGatewaySupport.class);

    NamespaceRef namespaceRef =
        new NamespaceRef(new CatalogRef("examples", "examples", null), "iceberg", List.of(), null);
    when(resource.resourceResolver.namespace("examples", "iceberg")).thenReturn(namespaceRef);

    Response response = Response.ok(Map.of("status", "ok")).build();
    when(resource.tableCommitService.commit(any())).thenReturn(response);

    TableRequests.Commit request =
        new TableRequests.Commit(List.of(Map.of("type", "assert-table-uuid")), List.of());

    Response actual =
        resource.commit("examples", "iceberg", "orders", null, "idem-1", "txn-1", request);

    assertSame(response, actual);
    verify(resource.commitTrafficLogger)
        .logRequest("POST", "/v1/examples/namespaces/iceberg/tables/orders", request);
    verify(resource.tableCommitService).commit(any(TableCommitService.CommitCommand.class));
    verify(resource.commitTrafficLogger)
        .logResponse(
            eq("POST"), eq("/v1/examples/namespaces/iceberg/tables/orders"), eq(200), any());
  }
}

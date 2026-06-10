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

import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitTrafficLogger;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.transaction.TransactionCommitService;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TableAdminResourceTest {
  @Test
  void commitTransactionLogsRequestAndResponse() {
    TableAdminResource resource = new TableAdminResource();
    resource.transactionCommitService = mock(TransactionCommitService.class);
    resource.commitTrafficLogger = mock(CommitTrafficLogger.class);
    resource.tableSupport = mock(TableGatewaySupport.class);

    Response response = Response.ok(Map.of("status", "ok")).build();
    when(resource.transactionCommitService.commit(any(), any(), any(), any())).thenReturn(response);

    TransactionCommitRequest request =
        new TransactionCommitRequest(
            List.of(
                new TransactionCommitRequest.TableChange(
                    new TableIdentifierDto(List.of("iceberg"), "orders"), List.of(), List.of())));

    Response actual = resource.commitTransaction("examples", "idem-1", request);

    assertSame(response, actual);
    verify(resource.commitTrafficLogger)
        .logRequest("POST", "/v1/examples/transactions/commit", request);
    verify(resource.transactionCommitService)
        .commit("examples", "idem-1", request, resource.tableSupport);
    verify(resource.commitTrafficLogger)
        .logResponse(eq("POST"), eq("/v1/examples/transactions/commit"), eq(200), any());
  }
}

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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.RequestContextFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.core.Response;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TransactionCommitServiceTest {
  private final TransactionCommitService service = new TransactionCommitService();
  private final AccountContext accountContext = Mockito.mock(AccountContext.class);
  private final RequestContextFactory requestContextFactory =
      Mockito.mock(RequestContextFactory.class);
  private final TableLifecycleService tableLifecycleService =
      Mockito.mock(TableLifecycleService.class);
  private final TableCommitService tableCommitService = Mockito.mock(TableCommitService.class);
  private final TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);
  private final CommitOperationTracker tracker = new CommitOperationTracker();

  @BeforeEach
  void setUp() {
    service.accountContext = accountContext;
    service.requestContextFactory = requestContextFactory;
    service.tableLifecycleService = tableLifecycleService;
    service.tableCommitService = tableCommitService;
    service.commitOperationTracker = tracker;
    tracker.mapper = new ObjectMapper();

    when(accountContext.getAccountId()).thenReturn("acct");
    when(requestContextFactory.catalog("foo"))
        .thenReturn(
            new CatalogRequestContext(
                "foo", "foo", ResourceId.newBuilder().setAccountId("acct").setId("cat").build()));
    when(tableLifecycleService.resolveNamespaceId("foo", List.of("db")))
        .thenReturn(ResourceId.newBuilder().setAccountId("acct").setId("ns").build());
    when(tableCommitService.commit(any())).thenReturn(Response.noContent().build());
  }

  @Test
  void commitReplaysForSameIdempotencyPayload() {
    TransactionCommitRequest request =
        new TransactionCommitRequest(
            List.of(
                new TransactionCommitRequest.TableChange(
                    new TableIdentifierDto(List.of("db"), "orders"), null, List.of(), List.of())));

    Response first = service.commit("foo", "idem-1", null, request, tableSupport);
    Response second = service.commit("foo", "idem-1", null, request, tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), first.getStatus());
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), second.getStatus());
    verify(tableCommitService, times(1)).commit(any());
  }

  @Test
  void commitRejectsIdempotencyPayloadMismatch() {
    TransactionCommitRequest firstReq =
        new TransactionCommitRequest(
            List.of(
                new TransactionCommitRequest.TableChange(
                    new TableIdentifierDto(List.of("db"), "orders"), null, List.of(), List.of())));
    TransactionCommitRequest secondReq =
        new TransactionCommitRequest(
            List.of(
                new TransactionCommitRequest.TableChange(
                    new TableIdentifierDto(List.of("db"), "orders2"), null, List.of(), List.of())));

    Response first = service.commit("foo", "idem-2", null, firstReq, tableSupport);
    Response second = service.commit("foo", "idem-2", null, secondReq, tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), first.getStatus());
    assertEquals(Response.Status.CONFLICT.getStatusCode(), second.getStatus());
    verify(tableCommitService, times(1)).commit(any());
  }
}

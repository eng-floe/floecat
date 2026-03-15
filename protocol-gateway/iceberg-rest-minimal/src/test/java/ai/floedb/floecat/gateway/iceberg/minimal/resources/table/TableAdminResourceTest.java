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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.gateway.iceberg.minimal.api.request.RenameRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableBackend;
import ai.floedb.floecat.gateway.iceberg.minimal.services.transaction.TransactionCommitService;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TableAdminResourceTest {
  private final TableBackend backend = Mockito.mock(TableBackend.class);
  private final TransactionCommitService transactionCommitService =
      Mockito.mock(TransactionCommitService.class);
  private final TableAdminResource resource =
      new TableAdminResource(backend, transactionCommitService);

  @Test
  void renameReturns204() {
    assertEquals(
        204,
        resource
            .rename(
                "foo",
                new RenameRequest(
                    new RenameRequest.TableIdentifierBody(List.of("db"), "orders"),
                    new RenameRequest.TableIdentifierBody(List.of("analytics"), "orders_new")))
            .getStatus());

    verify(backend).rename("foo", List.of("db"), "orders", List.of("analytics"), "orders_new");
  }

  @Test
  void transactionCommitDelegatesToService() {
    when(transactionCommitService.commit(Mockito.eq("foo"), Mockito.eq("idem-1"), Mockito.any()))
        .thenReturn(Response.noContent().build());

    assertEquals(
        204,
        resource
            .commitTransaction(
                "foo",
                "idem-1",
                new TransactionCommitRequest(
                    List.of(
                        new TransactionCommitRequest.TableChange(
                            new ai.floedb.floecat.gateway.iceberg.minimal.api.dto
                                .TableIdentifierDto(List.of("db"), "orders"),
                            List.of(Map.of("type", "assert-create")),
                            List.of()))))
            .getStatus());
  }
}

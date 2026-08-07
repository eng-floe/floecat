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
package ai.floedb.floecat.service.catalog.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import org.junit.jupiter.api.Test;

class RelationNamespaceScopingTest {

  @Test
  void tableReclaimNamespaceIsScopedToThePrincipalAccount() {
    var principal = principal("acct");
    var service = new TableServiceImpl();
    service.principal = principal;

    var namespace =
        service.namespaceOf(
            TableSpec.newBuilder()
                .setNamespaceId(ResourceId.newBuilder().setId("ns"))
                .setCatalogId(ResourceId.newBuilder().setId("cat"))
                .build());

    assertEquals("acct", namespace.getResourceId().getAccountId());
    assertEquals("acct", namespace.getCatalogId().getAccountId());
  }

  @Test
  void viewReclaimNamespaceIsScopedToThePrincipalAccount() {
    var principal = principal("acct");
    var service = new ViewServiceImpl();
    service.principal = principal;

    var namespace =
        service.namespaceOf(
            ViewSpec.newBuilder()
                .setNamespaceId(ResourceId.newBuilder().setId("ns"))
                .setCatalogId(ResourceId.newBuilder().setId("cat"))
                .build());

    assertEquals("acct", namespace.getResourceId().getAccountId());
    assertEquals("acct", namespace.getCatalogId().getAccountId());
  }

  @Test
  void tableCreateAndUpdateValuesPersistTheScopedNamespace() {
    var scoped = ResourceId.newBuilder().setAccountId("acct").setId("ns").build();
    var raw = ResourceId.newBuilder().setAccountId("forged").setId("ns").build();

    var create =
        TableServiceImpl.normalizedForPersistence(
            TableSpec.newBuilder().setNamespaceId(raw).setDisplayName("raw").build(),
            "normalized",
            scoped);
    var update =
        TableServiceImpl.normalizedForPersistence(
            Table.newBuilder().setNamespaceId(raw).build(), scoped);

    assertEquals(scoped, create.getNamespaceId());
    assertEquals("normalized", create.getDisplayName());
    assertEquals(scoped, update.getNamespaceId());
  }

  @Test
  void viewCreateAndUpdateValuesPersistTheScopedNamespace() {
    var scoped = ResourceId.newBuilder().setAccountId("acct").setId("ns").build();
    var raw = ResourceId.newBuilder().setAccountId("forged").setId("ns").build();

    var create =
        ViewServiceImpl.normalizedForPersistence(
            ViewSpec.newBuilder().setNamespaceId(raw).setDisplayName("raw").build(),
            "normalized",
            scoped);
    var update =
        ViewServiceImpl.normalizedForPersistence(
            View.newBuilder().setNamespaceId(raw).build(), scoped);

    assertEquals(scoped, create.getNamespaceId());
    assertEquals("normalized", create.getDisplayName());
    assertEquals(scoped, update.getNamespaceId());
  }

  private static PrincipalProvider principal(String accountId) {
    var principal = mock(PrincipalProvider.class);
    var context = mock(PrincipalContext.class);
    when(context.getAccountId()).thenReturn(accountId);
    when(principal.get()).thenReturn(context);
    return principal;
  }
}

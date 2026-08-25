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

package ai.floedb.floecat.service.connector.impl;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import org.junit.jupiter.api.Test;

class ConnectorsImplCreateConnectorTest {

  @Test
  void accountDeletionFenceCompensatesStoredCredentials() {
    var service = new ConnectorsImpl();
    service.connectorRepo = mock(ConnectorRepository.class);
    service.credentialResolver = mock(CredentialResolver.class);
    var connector =
        Connector.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("connector-1")
                    .setKind(ResourceKind.RK_CONNECTOR))
            .build();
    var deleting = new BaseResourceRepository.AccountDeletionInProgressException("acct");
    doThrow(deleting).when(service.connectorRepo).create(connector);

    var thrown =
        assertThrows(
            BaseResourceRepository.AccountDeletionInProgressException.class,
            () ->
                service.createConnectorWithCredentialCompensation(
                    connector, true, "acct", "connector-1"));

    assertSame(deleting, thrown);
    verify(service.credentialResolver).delete("acct", "connector-1");
  }
}

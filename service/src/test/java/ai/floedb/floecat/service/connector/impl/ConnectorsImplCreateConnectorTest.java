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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.service.credentials.DefaultCredentialResolver;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import org.junit.jupiter.api.Test;

class ConnectorsImplCreateConnectorTest {

  @Test
  void guardFailureDeletesTheCredentialStoredBeforePublication() {
    var service = new ConnectorsImpl();
    service.connectorRepo = mock(ConnectorRepository.class);
    service.credentialResolver = mock(DefaultCredentialResolver.class);
    var connector = Connector.getDefaultInstance();
    var failure = new BaseResourceRepository.BatchGuardFailedException("account changed");
    doThrow(failure).when(service.connectorRepo).create(connector, BatchGuard.NONE);

    assertThatThrownBy(
            () ->
                service.createWithCredentialCleanup(
                    connector, BatchGuard.NONE, "acct", "connector-1", true, null))
        .isSameAs(failure);
    verify(service.credentialResolver).delete("acct", "connector-1");
  }
}

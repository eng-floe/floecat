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

package ai.floedb.floecat.connector.common.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RefreshingAwsClientTest {

  @Test
  void callRefreshesAndRetriesCheckedConnectionPoolShutdown() throws IOException {
    AtomicInteger created = new AtomicInteger();
    List<TestClient> clients = new ArrayList<>();
    RefreshingAwsClient<TestClient> refreshing =
        new RefreshingAwsClient<>(
            () -> {
              TestClient client = new TestClient(created.incrementAndGet());
              clients.add(client);
              return client;
            });

    AtomicInteger calls = new AtomicInteger();
    String result =
        refreshing.call(
            client -> {
              if (calls.incrementAndGet() == 1) {
                throw new IOException("Connection pool shut down");
              }
              return "client-" + client.id;
            });

    assertEquals("client-2", result);
    assertEquals(2, calls.get());
    assertEquals(2, created.get());
    assertTrue(clients.get(0).closed);
    assertFalse(clients.get(1).closed);
  }

  private static final class TestClient implements AutoCloseable {
    private final int id;
    private boolean closed;

    private TestClient(int id) {
      this.id = id;
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}

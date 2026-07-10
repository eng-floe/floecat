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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RefreshingAwsClientTest {

  @Test
  void callRefreshesAndRetriesCheckedClosedPoolIoException() throws Exception {
    List<TestClient> created = new ArrayList<>();
    AtomicInteger attempts = new AtomicInteger();

    try (RefreshingAwsClient<TestClient> client =
        new RefreshingAwsClient<>(
            () -> {
              TestClient next = new TestClient(created.size() + 1);
              created.add(next);
              return next;
            })) {
      String result =
          client.call(
              current -> {
                if (attempts.getAndIncrement() == 0) {
                  throw new IOException("Connection pool shut down");
                }
                return "client-" + current.id;
              });

      assertThat(result).isEqualTo("client-2");
    }

    assertThat(attempts).hasValue(2);
    assertThat(created).hasSize(2);
    assertThat(created.get(0).closed).isTrue();
    assertThat(created.get(1).closed).isTrue();
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

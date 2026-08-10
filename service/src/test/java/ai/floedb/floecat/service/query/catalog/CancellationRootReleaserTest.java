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

package ai.floedb.floecat.service.query.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.service.query.QueryContextStore;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CancellationRootReleaserTest {

  @Test
  void emptyPinSetDoesNotDispatchCleanup() {
    QueryContextStore queryStore = mock(QueryContextStore.class);
    AtomicInteger dispatches = new AtomicInteger();
    CancellationRootReleaser releaser =
        new CancellationRootReleaser(
            queryStore,
            task -> {
              dispatches.incrementAndGet();
              task.run();
            });

    releaser.release("query", RelationPinSet.getDefaultInstance());

    assertThat(dispatches).hasValue(0);
    verifyNoInteractions(queryStore);
  }
}

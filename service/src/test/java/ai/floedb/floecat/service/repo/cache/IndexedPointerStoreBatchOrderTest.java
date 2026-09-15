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

package ai.floedb.floecat.service.repo.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore.CasDelete;
import ai.floedb.floecat.storage.spi.PointerStore.CasOp;
import ai.floedb.floecat.storage.spi.PointerStore.CasUpsert;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * A rename removes one name and adds another in one batch. Readers hold only the partition read
 * lock, so they can observe the publish loop part-way through — and absence in this index is
 * authoritative. Showing both names is survivable; showing neither is a wrong answer.
 */
class IndexedPointerStoreBatchOrderTest {

  private static final String ACCT = "acct-1";

  @Test
  @SuppressWarnings("unchecked")
  void aBatchAddsEveryNameBeforeItRemovesAny() {
    String oldName = Keys.relationPointerByName(ACCT, "cat", "ns", "before");
    String newName = Keys.relationPointerByName(ACCT, "cat", "ns", "after");

    List<String> order = new ArrayList<>();
    PlanningPointerIndex index = mock(PlanningPointerIndex.class);
    doAnswer(call -> order.add("publish " + call.getArgument(0)))
        .when(index)
        .publish(anyString(), any());
    doAnswer(call -> order.add("remove " + call.getArgument(0))).when(index).remove(anyString());
    when(index.mutateKeys(any(), any(), any()))
        .thenAnswer(
            call -> {
              Object result = ((Supplier<Object>) call.getArgument(1)).get();
              ((Consumer<Object>) call.getArgument(2)).accept(result);
              return result;
            });

    var durable = new InMemoryPointerStore();
    durable.compareAndSet(oldName, 0L, Pointer.newBuilder().setKey(oldName).setVersion(1L).build());
    var subject = new IndexedPointerStore(durable, index);
    List<CasOp> rename =
        List.of(
            new CasDelete(oldName, 1L),
            new CasUpsert(newName, 0L, Pointer.newBuilder().setKey(newName).build()));

    subject.compareAndSetBatch(rename);

    assertThat(order)
        .as("the removal must not land before the insertion it renames to")
        .containsExactly("publish " + newName, "remove " + oldName);
  }
}

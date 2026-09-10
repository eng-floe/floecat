/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.repo.cache;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PlanningPointerIndexTest {
  @Test
  void completeAccountIndexMakesMissingPlannerPointerAuthoritative() {
    CountingStore durable = new CountingStore();
    String present = Keys.tablePointerById("acct", "table");
    durable.compareAndSet(present, 0L, pointer(present, "s3://table"));
    IndexedPointerStore store = new IndexedPointerStore(durable, new PlanningPointerIndex(durable));

    assertThat(store.get(present)).isPresent();
    int readsAfterLoad = durable.pointReads.get();
    assertThat(store.get(Keys.tablePointerById("acct", "missing"))).isEmpty();
    assertThat(durable.pointReads.get()).isEqualTo(readsAfterLoad);
  }

  @Test
  void successfulMutationPublishesWhileTheAccountPartitionIsLocked() {
    CountingStore durable = new CountingStore();
    PlanningPointerIndex index = new PlanningPointerIndex(durable);
    IndexedPointerStore store = new IndexedPointerStore(durable, index);
    String key = Keys.tablePointerById("acct", "table");

    assertThat(store.get(key)).isEmpty();
    assertThat(store.compareAndSet(key, 0L, pointer(key, "s3://new"))).isTrue();
    assertThat(store.get(key).map(Pointer::getBlobUri)).contains("s3://new");
  }

  @Test
  void operationalPointersStayOnDurablePath() {
    CountingStore durable = new CountingStore();
    String key = Keys.transactionPointerById("acct", "tx");
    durable.compareAndSet(key, 0L, pointer(key, "s3://transaction"));
    IndexedPointerStore store = new IndexedPointerStore(durable, new PlanningPointerIndex(durable));

    assertThat(store.get(key)).isPresent();
    assertThat(durable.pointReads).hasValue(1);
  }

  private static Pointer pointer(String key, String uri) {
    return Pointer.newBuilder().setKey(key).setBlobUri(uri).build();
  }

  private static final class CountingStore extends InMemoryPointerStore {
    private final AtomicInteger pointReads = new AtomicInteger();

    @Override
    public synchronized Optional<Pointer> get(String key) {
      pointReads.incrementAndGet();
      return super.get(key);
    }

    @Override
    public synchronized Optional<Pointer> getConsistent(String key) {
      pointReads.incrementAndGet();
      return super.getConsistent(key);
    }
  }
}

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

package ai.floedb.floecat.service.storage;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.storage.StoreReadObserver.Operation;
import ai.floedb.floecat.service.storage.StoreReadObserver.ReadCall;
import ai.floedb.floecat.service.storage.StoreReadObserver.Store;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/** Adds read observability to whichever pointer-store adapter the deployment selects. */
@Decorator
@Priority(Interceptor.Priority.APPLICATION)
public final class ObservedPointerStore implements PointerStore {
  private static final long NO_BYTES = -1L;

  private final PointerStore delegate;
  private final StoreReadObserver observer;

  @Inject
  public ObservedPointerStore(@Delegate @Any PointerStore delegate, StoreReadObserver observer) {
    this.delegate = delegate;
    this.observer = observer;
  }

  @Override
  public Optional<Pointer> get(String key) {
    return observe(Operation.GET, 1, Collections.singletonList(key), () -> delegate.get(key));
  }

  @Override
  public Map<String, Pointer> getBatch(List<String> keys) {
    if (keys == null || keys.isEmpty()) {
      return delegate.getBatch(keys);
    }
    return observe(Operation.GET_BATCH, keys.size(), keys, () -> delegate.getBatch(keys));
  }

  @Override
  public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
    return delegate.compareAndSet(key, expectedVersion, next);
  }

  @Override
  public boolean delete(String key) {
    return delegate.delete(key);
  }

  @Override
  public boolean compareAndDelete(String key, long expectedVersion) {
    return delegate.compareAndDelete(key, expectedVersion);
  }

  @Override
  public boolean compareAndSetBatch(List<CasOp> ops) {
    return delegate.compareAndSetBatch(ops);
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    return observe(
        Operation.SCAN_PREFIX,
        0,
        Collections.singletonList(prefix),
        () -> delegate.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut));
  }

  @Override
  public List<Pointer> listPointersByPrefixConsistent(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    return observe(
        Operation.SCAN_PREFIX_CONSISTENT,
        0,
        Collections.singletonList(prefix),
        () -> delegate.listPointersByPrefixConsistent(prefix, limit, pageToken, nextTokenOut));
  }

  @Override
  public String pageTokenAfterKey(String key) {
    return delegate.pageTokenAfterKey(key);
  }

  @Override
  public int deleteByPrefix(String prefix) {
    return delegate.deleteByPrefix(prefix);
  }

  @Override
  public int deleteByPrefixExcluding(String prefix, String excludedKey) {
    return delegate.deleteByPrefixExcluding(prefix, excludedKey);
  }

  @Override
  public int countByPrefix(String prefix) {
    return observe(
        Operation.COUNT_PREFIX,
        0,
        Collections.singletonList(prefix),
        () -> delegate.countByPrefix(prefix));
  }

  @Override
  public int countByPrefixConsistent(String prefix) {
    return observe(
        Operation.COUNT_PREFIX_CONSISTENT,
        0,
        Collections.singletonList(prefix),
        () -> delegate.countByPrefixConsistent(prefix));
  }

  @Override
  public boolean isEmpty() {
    return observe(Operation.IS_EMPTY, 0, List.of(), delegate::isEmpty);
  }

  @Override
  public void dump(String header) {
    delegate.dump(header);
  }

  private <T> T observe(Operation operation, int items, List<String> targets, Supplier<T> body) {
    return StoreReadInstrumentation.observe(
        observer,
        new ReadCall(
            Store.POINTER, operation, items, observer.capturesTargets() ? targets : List.of()),
        body,
        ignored -> NO_BYTES);
  }
}

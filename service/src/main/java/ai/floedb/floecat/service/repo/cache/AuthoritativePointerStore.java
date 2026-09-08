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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A pointer-store view whose ordinary reads are authoritative.
 *
 * <p>Mutation, GC and reconciliation code receives this view as the unqualified {@link
 * PointerStore}. It therefore cannot accidentally use a cached prerequisite by calling the most
 * obvious read method. Writes still pass through the wrapped {@link CachingPointerStore}, so its
 * publication and race fencing remain the single write path.
 */
public final class AuthoritativePointerStore implements PointerStore {
  private final PointerStore delegate;

  private AuthoritativePointerStore(PointerStore delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  /** Return an authoritative view without stacking duplicate views. */
  public static PointerStore of(PointerStore delegate) {
    return delegate instanceof AuthoritativePointerStore
        ? delegate
        : new AuthoritativePointerStore(delegate);
  }

  @Override
  public Optional<Pointer> get(String key) {
    return delegate.getConsistent(key);
  }

  @Override
  public Optional<Pointer> getConsistent(String key) {
    return delegate.getConsistent(key);
  }

  @Override
  public Map<String, Pointer> getBatch(List<String> keys) {
    return delegate.getBatchConsistent(keys);
  }

  @Override
  public Map<String, Pointer> getBatchConsistent(List<String> keys) {
    return delegate.getBatchConsistent(keys);
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    return delegate.listPointersByPrefixConsistent(prefix, limit, pageToken, nextTokenOut);
  }

  @Override
  public List<Pointer> listPointersByPrefixConsistent(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    return delegate.listPointersByPrefixConsistent(prefix, limit, pageToken, nextTokenOut);
  }

  @Override
  public int countByPrefix(String prefix) {
    return delegate.countByPrefixConsistent(prefix);
  }

  @Override
  public int countByPrefixConsistent(String prefix) {
    return delegate.countByPrefixConsistent(prefix);
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
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public void dump(String header) {
    delegate.dump(header);
  }
}

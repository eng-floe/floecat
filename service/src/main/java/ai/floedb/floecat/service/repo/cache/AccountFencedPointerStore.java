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
import ai.floedb.floecat.service.account.AccountGcAuthority;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Account mutation admission at the one store seam every durable pointer publication crosses.
 *
 * <p>Reads are transparent. Account-scoped writes hold an authority permit through the durable
 * mutation and any write-through publication performed by the delegate. Global account-directory
 * and reconciler-index pointers remain infrastructure state and are deliberately not tied to one
 * account owner.
 */
public final class AccountFencedPointerStore implements PointerStore {

  private final PointerStore delegate;
  private final AccountGcAuthority authority;

  public AccountFencedPointerStore(PointerStore delegate, AccountGcAuthority authority) {
    this.delegate = java.util.Objects.requireNonNull(delegate, "delegate");
    this.authority = java.util.Objects.requireNonNull(authority, "authority");
  }

  @Override
  public Optional<Pointer> get(String key) {
    return delegate.get(key);
  }

  @Override
  public Optional<Pointer> getConsistent(String key) {
    return delegate.getConsistent(key);
  }

  @Override
  public Map<String, Pointer> getBatch(List<String> keys) {
    return delegate.getBatch(keys);
  }

  @Override
  public Map<String, Pointer> getBatchConsistent(List<String> keys) {
    return delegate.getBatchConsistent(keys);
  }

  @Override
  public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
    return mutate(List.of(key), () -> delegate.compareAndSet(key, expectedVersion, next));
  }

  @Override
  public boolean delete(String key) {
    return mutate(List.of(key), () -> delegate.delete(key));
  }

  @Override
  public boolean compareAndDelete(String key, long expectedVersion) {
    return mutate(List.of(key), () -> delegate.compareAndDelete(key, expectedVersion));
  }

  @Override
  public boolean compareAndSetBatch(List<CasOp> ops) {
    List<String> keys = ops == null ? List.of() : ops.stream().map(CasOp::key).toList();
    return mutate(keys, () -> delegate.compareAndSetBatch(ops));
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    return delegate.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
  }

  @Override
  public List<Pointer> listPointersByPrefixConsistent(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    return delegate.listPointersByPrefixConsistent(prefix, limit, pageToken, nextTokenOut);
  }

  @Override
  public String pageTokenAfterKey(String key) {
    return delegate.pageTokenAfterKey(key);
  }

  @Override
  public int deleteByPrefix(String prefix) {
    return mutate(List.of(prefix), () -> delegate.deleteByPrefix(prefix));
  }

  @Override
  public int deleteByPrefixExcluding(String prefix, String excludedKey) {
    return mutate(List.of(prefix), () -> delegate.deleteByPrefixExcluding(prefix, excludedKey));
  }

  @Override
  public int countByPrefix(String prefix) {
    return delegate.countByPrefix(prefix);
  }

  @Override
  public int countByPrefixConsistent(String prefix) {
    return delegate.countByPrefixConsistent(prefix);
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public void dump(String header) {
    delegate.dump(header);
  }

  private <T> T mutate(List<String> keys, Supplier<T> mutation) {
    List<AccountGcAuthority.Permit> permits = new ArrayList<>();
    try {
      LinkedHashSet<String> accountIds = new LinkedHashSet<>();
      keys.stream()
          .map(AccountFencedPointerStore::scopedAccountId)
          .flatMap(Optional::stream)
          .sorted()
          .forEach(accountIds::add);
      for (String accountId : accountIds) {
        permits.add(authority.admitMutation(accountId));
      }
      return mutation.get();
    } finally {
      for (int index = permits.size() - 1; index >= 0; index--) {
        permits.get(index).close();
      }
    }
  }

  private static Optional<String> scopedAccountId(String keyOrPrefix) {
    String cleanupRoot = Keys.catalogIntegrationCredentialCleanupPrefix();
    if (keyOrPrefix != null && keyOrPrefix.startsWith(cleanupRoot)) {
      int start = cleanupRoot.length();
      int end = keyOrPrefix.indexOf('/', start);
      String encoded = keyOrPrefix.substring(start, end < 0 ? keyOrPrefix.length() : end);
      return encoded.isEmpty() ? Optional.empty() : Optional.of(Keys.decodeSegment(encoded));
    }
    String root = Keys.accountRootPrefix();
    if (keyOrPrefix == null || !keyOrPrefix.startsWith(root)) {
      return Optional.empty();
    }
    int end = keyOrPrefix.indexOf('/', root.length());
    String encoded = keyOrPrefix.substring(root.length(), end < 0 ? keyOrPrefix.length() : end);
    // /accounts/by-{id,name}/... are global account-directory state. Both /accounts/<id> and its
    // usual trailing-slash form identify the account-owned namespace and must be fenced.
    if (encoded.isEmpty() || Keys.isReservedAccountDirectorySegment(encoded)) {
      return Optional.empty();
    }
    return Optional.of(Keys.decodeSegment(encoded));
  }
}

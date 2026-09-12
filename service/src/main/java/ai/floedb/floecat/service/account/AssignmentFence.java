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

package ai.floedb.floecat.service.account;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * The store fence: every account-scoped write by an owning process carries {@code
 * CasCheck(assignment fence, remembered version)}, so a process that lost the account cannot
 * commit. Sits once beneath {@code IndexedPointerStore}, leaving repositories unchanged.
 *
 * <p>A single-key write becomes a two-item transaction; a batch gains one check per account it
 * writes; a prefix delete, which cannot be transactional, is preceded by one consistent read.
 * Outside managed mode, and for accounts this process does not own, every call passes through.
 */
public final class AssignmentFence implements PointerStore {
  private static final int DELETE_ATTEMPTS = 3;

  private final PointerStore delegate;
  private final AccountAssignment assignment;

  public AssignmentFence(PointerStore delegate, AccountAssignment assignment) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.assignment = Objects.requireNonNull(assignment, "assignment");
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
    Optional<Check> check = checkFor(key);
    return check.isEmpty()
        ? delegate.compareAndSet(key, expectedVersion, next)
        : fenced(List.of(check.get()), List.of(new CasUpsert(key, expectedVersion, next)));
  }

  @Override
  public boolean compareAndDelete(String key, long expectedVersion) {
    Optional<Check> check = checkFor(key);
    return check.isEmpty()
        ? delegate.compareAndDelete(key, expectedVersion)
        : fenced(List.of(check.get()), List.of(new CasDelete(key, expectedVersion)));
  }

  /** Unconditional in the SPI, so the fenced form re-reads the version it deletes. */
  @Override
  public boolean delete(String key) {
    Optional<Check> check = checkFor(key);
    if (check.isEmpty()) {
      return delegate.delete(key);
    }
    for (int attempt = 0; attempt < DELETE_ATTEMPTS; attempt++) {
      Optional<Pointer> current = delegate.getConsistent(key);
      if (current.isEmpty()) {
        return false;
      }
      if (fenced(List.of(check.get()), List.of(new CasDelete(key, current.get().getVersion())))) {
        return true;
      }
      if (assignment.fenceVersion(check.get().accountId()).isEmpty()) {
        return false;
      }
    }
    return false;
  }

  @Override
  public boolean compareAndSetBatch(List<CasOp> ops) {
    List<Check> checks = checksFor(ops);
    return checks.isEmpty() ? delegate.compareAndSetBatch(ops) : fenced(checks, ops);
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
    checkFor(prefix).ifPresent(this::requireFenceUnchanged);
    return delegate.deleteByPrefix(prefix);
  }

  /** Account teardown; the marker transaction that precedes it already carried the fence. */
  @Override
  public int deleteByPrefixExcluding(String prefix, String excludedKey) {
    return delegate.deleteByPrefixExcluding(prefix, excludedKey);
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

  // -----------------------------------------------------------------------------------------

  private record Check(String accountId, long version) {}

  private boolean fenced(List<Check> checks, List<CasOp> ops) {
    List<CasOp> fencedOps = new ArrayList<>(ops.size() + checks.size());
    for (Check check : checks) {
      fencedOps.add(new CasCheck(Keys.accountAssignmentFence(check.accountId()), check.version()));
    }
    fencedOps.addAll(ops);
    boolean committed = delegate.compareAndSetBatch(fencedOps);
    if (!committed) {
      checks.forEach(check -> assignment.fenceRejected(check.accountId(), check.version()));
    }
    return committed;
  }

  private void requireFenceUnchanged(Check check) {
    Optional<Pointer> fence =
        delegate.getConsistent(Keys.accountAssignmentFence(check.accountId()));
    if (fence.isEmpty() || fence.get().getVersion() != check.version()) {
      assignment.fenceRejected(check.accountId(), check.version());
      throw new StorageAbortRetryableException(
          "account fence rejected prefix delete: " + check.accountId());
    }
  }

  /** One check per owned account this batch writes to; empty when nothing here is fenced. */
  private List<Check> checksFor(List<CasOp> ops) {
    if (ops == null || ops.isEmpty()) {
      return List.of();
    }
    Map<String, Check> byAccount = new LinkedHashMap<>();
    for (CasOp op : ops) {
      if (op instanceof CasCheck || op instanceof CasCheckAbsent) {
        continue;
      }
      accountScope(op.key())
          .filter(accountId -> !byAccount.containsKey(accountId))
          .flatMap(this::checkForAccount)
          .ifPresent(check -> byAccount.put(check.accountId(), check));
    }
    return List.copyOf(byAccount.values());
  }

  private Optional<Check> checkFor(String keyOrPrefix) {
    return accountScope(keyOrPrefix).flatMap(this::checkForAccount);
  }

  private Optional<Check> checkForAccount(String accountId) {
    OptionalLong version = assignment.fenceVersion(accountId);
    return version.isEmpty()
        ? Optional.empty()
        : Optional.of(new Check(accountId, version.getAsLong()));
  }

  /**
   * Account whose namespace ({@code /accounts/<id>/...}) the key or prefix belongs to. Directory
   * keys ({@code by-id}, {@code by-name}) belong to no account, as in the pointer index, because
   * both ask {@link Keys}. The two differ on one key by design: the bare {@code /accounts/<id>}
   * record is global to the index, since it is created before any process owns the account, but is
   * fenced here, so a superseded owner cannot rewrite it.
   */
  static Optional<String> accountScope(String keyOrPrefix) {
    Keys.PointerNamespace namespace = Keys.pointerNamespace(keyOrPrefix);
    if (namespace != Keys.PointerNamespace.PLANNER
        && namespace != Keys.PointerNamespace.OPERATIONAL) {
      return Optional.empty();
    }
    String remainder = keyOrPrefix.substring(Keys.accountRootPrefix().length());
    int slash = remainder.indexOf('/');
    String encoded = slash < 0 ? remainder : remainder.substring(0, slash);
    return encoded.isBlank() ? Optional.empty() : Optional.of(Keys.decodeSegment(encoded));
  }
}

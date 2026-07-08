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

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Shared {@link PointerStore} decorators used by repository behavioral tests. */
final class RepoTestPointerStores {

  private RepoTestPointerStores() {}

  /** Forwards every {@link PointerStore} call to a delegate; subclasses override one behavior. */
  abstract static class DelegatingPointerStore implements PointerStore {
    final PointerStore delegate;

    DelegatingPointerStore(PointerStore delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean compareAndSetBatch(List<CasOp> ops) {
      return delegate.compareAndSetBatch(ops);
    }

    @Override
    public Optional<Pointer> get(String key) {
      return delegate.get(key);
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
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      return delegate.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
    }

    @Override
    public int deleteByPrefix(String prefix) {
      return delegate.deleteByPrefix(prefix);
    }

    @Override
    public int countByPrefix(String prefix) {
      return delegate.countByPrefix(prefix);
    }

    @Override
    public boolean isEmpty() {
      return delegate.isEmpty();
    }

    @Override
    public String pageTokenAfterKey(String key) {
      return delegate.pageTokenAfterKey(key);
    }
  }

  /** Fails every batch CAS, simulating a transient storage error that aborts the transaction. */
  static final class FailingBatchPointerStore extends DelegatingPointerStore {
    FailingBatchPointerStore(PointerStore delegate) {
      super(delegate);
    }

    static final class InjectedBatchFailure extends RuntimeException {
      InjectedBatchFailure(String message) {
        super(message);
      }
    }

    @Override
    public boolean compareAndSetBatch(List<CasOp> ops) {
      throw new InjectedBatchFailure("injected batch storage failure");
    }
  }

  /**
   * Reports every batch CAS as a non-committing conflict, mimicking a DynamoDB {@code
   * TransactionConflict} / {@code ConditionalCheckFailed} cancellation: nothing is mutated and
   * {@code false} is returned. Lets a test drive the "batch said no, but read-back finds nothing"
   * transient path without a real concurrent writer.
   */
  static final class ConflictingBatchPointerStore extends DelegatingPointerStore {
    ConflictingBatchPointerStore(PointerStore delegate) {
      super(delegate);
    }

    @Override
    public boolean compareAndSetBatch(List<CasOp> ops) {
      return false;
    }
  }

  /** Counts legacy prefix scans so tests can assert O(1) index paths are used. */
  static final class CountingPrefixScanPointerStore extends DelegatingPointerStore {
    private int listPointersByPrefixCalls = 0;

    CountingPrefixScanPointerStore(PointerStore delegate) {
      super(delegate);
    }

    @Override
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      listPointersByPrefixCalls++;
      return super.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
    }

    int listPointersByPrefixCalls() {
      return listPointersByPrefixCalls;
    }
  }

  /** Mimics DynamoDB: a transaction with two operations on the same key is rejected. */
  static final class DuplicateKeyRejectingPointerStore extends DelegatingPointerStore {
    DuplicateKeyRejectingPointerStore(PointerStore delegate) {
      super(delegate);
    }

    @Override
    public boolean compareAndSetBatch(List<CasOp> ops) {
      Set<String> seen = new HashSet<>();
      for (CasOp op : ops) {
        String key = keyOf(op);
        if (!seen.add(key)) {
          throw new IllegalArgumentException("duplicate key in transactional batch: " + key);
        }
      }
      return delegate.compareAndSetBatch(ops);
    }

    private String keyOf(CasOp op) {
      if (op instanceof CasUpsert u) {
        return u.key();
      }
      if (op instanceof CasDelete d) {
        return d.key();
      }
      if (op instanceof CasCheck c) {
        return c.key();
      }
      return ((CasCheckAbsent) op).key();
    }
  }
}

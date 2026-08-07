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

package ai.floedb.floecat.service.repo.util;

import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;

/**
 * Extra preconditions a caller folds into a repository mutation's {@link
 * PointerStore#compareAndSetBatch} so the mutation commits only while some <em>other</em> pointer
 * is in an expected state.
 *
 * <p>The motivating case is publishing a child (a table, a view, a child namespace) into a parent
 * namespace that a concurrent {@code DeleteNamespace} is tearing down. A parent delete establishes
 * emptiness with a <em>scan</em>, and a scan can never be part of a CAS batch, so scan-then-delete
 * always leaves a window: a child published after the last scan but before the parent pointer is
 * removed is orphaned under a namespace that no longer exists.
 *
 * <p>A children marker closes that window only if the two sides actually contend on it inside their
 * respective atomic batches — the publishing side must <b>move</b> the marker in the same batch
 * that makes the child visible, and the delete side must <b>check</b> the marker in the same batch
 * that removes the parent pointer. Bumping the marker after the child is already visible (the shape
 * this interface replaces) makes it an advisory hint rather than a fence: both batches can succeed.
 *
 * <p>Guards are therefore always used in pairs:
 *
 * <ul>
 *   <li>the child-publishing mutation carries a guard whose ops <em>advance</em> the parent's
 *       marker and <em>check</em> that the parent pointer is still the exact version the caller
 *       resolved;
 *   <li>the parent delete carries a guard whose op <em>checks</em> the marker is still where its
 *       emptiness scan left it.
 * </ul>
 *
 * <p>Because both are single all-or-nothing batches over the same marker key, at most one of "child
 * published" and "parent deleted" can commit. The loser is told which it was via {@link
 * #reevaluate()}.
 *
 * <p>The parent can be either a namespace or a catalog. A namespace publish always carries the
 * catalog guard, and carries the namespace guard as well when it is nested, so both DeleteCatalog
 * and DeleteNamespace contend with the publish inside their respective atomic batches.
 */
public interface BatchGuard {

  /** What a caller should do after its batch failed to commit. */
  enum Outcome {
    /**
     * The guarded state is exactly as captured, so the guard is not what failed — the batch lost on
     * one of the mutation's own pointers and the caller classifies that conflict as usual.
     */
    HOLDS,
    /**
     * Benign contention: another writer moved the guarded marker (a sibling create in the same
     * namespace), but the guarded parent is still alive and unchanged. The guard has re-captured,
     * so {@link #ops()} now reflects the new state and the batch can simply be retried. This keeps
     * concurrent sibling creates as cheap as the pre-fence marker CAS loop instead of bouncing them
     * all the way out to an RPC-level retry.
     */
    RETRY,
    /**
     * The guarded parent is gone or is no longer the version the caller resolved. The mutation must
     * not be applied; a parent delete either won the race or is midway through winning it.
     */
    BROKEN
  }

  /**
   * Preconditions for the next batch attempt. Re-read after every {@link Outcome#RETRY}, so callers
   * must rebuild their op list from this on each attempt rather than caching it.
   */
  List<PointerStore.CasOp> ops();

  /** Re-reads the guarded state after a failed batch and reports what the caller should do. */
  Outcome reevaluate();

  /** Human-readable subject of the guard, for the exception raised on {@link Outcome#BROKEN}. */
  String describe();

  /**
   * Conjunction of {@code guards}: all of their ops become preconditions of the same batch, so it
   * is only as free to commit as the strictest of them.
   *
   * <p>A mutation can owe an answer to more than one piece of state at once. A namespace relocation
   * publishes into its destination parent while vacating a path its own children are indexed under,
   * so it has to fence the destination <em>and</em> hold the source still; either one alone leaves
   * a window. Op order does not matter, because the batch is all-or-nothing.
   */
  static BatchGuard all(BatchGuard... guards) {
    var live = java.util.Arrays.stream(guards).filter(g -> g != null && g != NONE).toList();
    if (live.isEmpty()) {
      return NONE;
    }
    if (live.size() == 1) {
      return live.get(0);
    }
    return new BatchGuard() {
      @Override
      public List<PointerStore.CasOp> ops() {
        return live.stream().flatMap(guard -> guard.ops().stream()).toList();
      }

      @Override
      public Outcome reevaluate() {
        // Every guard re-reads, even once the verdict is settled: a RETRY means that guard has
        // re-captured, and short-circuiting past the others would leave their ops stale for the
        // next attempt. BROKEN outranks RETRY — one unrecoverable guard makes the whole batch
        // unrecoverable, however benign the others turned out to be.
        Outcome verdict = Outcome.HOLDS;
        for (var guard : live) {
          Outcome outcome = guard.reevaluate();
          if (outcome == Outcome.BROKEN) {
            verdict = Outcome.BROKEN;
          } else if (outcome == Outcome.RETRY && verdict != Outcome.BROKEN) {
            verdict = Outcome.RETRY;
          }
        }
        return verdict;
      }

      @Override
      public String describe() {
        return live.stream()
            .map(BatchGuard::describe)
            .collect(java.util.stream.Collectors.joining(" and "));
      }
    };
  }

  /** A guard that constrains nothing, for the unguarded overloads of every mutation. */
  BatchGuard NONE =
      new BatchGuard() {
        @Override
        public List<PointerStore.CasOp> ops() {
          return List.of();
        }

        @Override
        public Outcome reevaluate() {
          return Outcome.HOLDS;
        }

        @Override
        public String describe() {
          return "none";
        }
      };
}

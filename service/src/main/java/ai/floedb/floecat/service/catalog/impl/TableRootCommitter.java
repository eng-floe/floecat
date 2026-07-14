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

package ai.floedb.floecat.service.catalog.impl;

import ai.floedb.floecat.catalog.rpc.TableRoot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

/**
 * The single owner of every {@link TableRoot} mutation. A commit is: read the current root (version
 * before value, so the whole attempt decides against one observed version), apply the caller's
 * mutator to it, write the new immutable root blob, and CAS the per-table pointer. A lost CAS
 * re-runs the mutator against the winner's root, so concurrent commits — snapshot ingest, a stats
 * finalize, a DDL — merge instead of clobbering: the mutator is a function of the current root,
 * never a prebuilt value.
 *
 * <p>Unlike the derived-state publishers this replaces, a root commit IS the mutation: failure must
 * fail the calling operation before it is acknowledged, so terminal repository errors and exhausted
 * retries throw {@link CommitFailedException} rather than being absorbed.
 *
 * <p>Mutators may write content-addressed sub-blobs (manifest pages) before returning — those
 * writes are idempotent, so a retried mutator converges on the same URIs. Mutators must not carry
 * state across invocations.
 */
@ApplicationScoped
public class TableRootCommitter {

  private static final int MAX_COMMIT_ATTEMPTS = 4;

  private final TableRootRepository roots;
  private final TableRootSynthesizer synthesizer;

  @Inject
  public TableRootCommitter(TableRootRepository roots, TableRootSynthesizer synthesizer) {
    this.roots = roots;
    this.synthesizer = synthesizer;
  }

  /** Without legacy synthesis (unit tests exercising pure commit semantics). */
  public TableRootCommitter(TableRootRepository roots) {
    this(roots, null);
  }

  /** A root commit could not be applied; the calling mutation must fail. */
  public static final class CommitFailedException extends RuntimeException {
    CommitFailedException(String message, Throwable cause) {
      super(message, cause);
    }

    CommitFailedException(String message) {
      super(message);
    }
  }

  /**
   * Builds the desired next root from the current one. Return {@code null} to signal a no-op
   * (nothing to commit); the committer then returns the current root unchanged. {@code root_seq}
   * and {@code committed_at} are stamped by the committer — mutators must not manage them.
   */
  @FunctionalInterface
  public interface RootMutator {
    TableRoot apply(Optional<TableRoot> current);
  }

  /**
   * Materializes the table's root without mutating it: a stored root is returned as-is, a legacy
   * table gets its history synthesized and committed, and an unknown table yields empty. The
   * read-side entry point for lazy migration.
   */
  public Optional<TableRoot> ensureRoot(ResourceId tableId) {
    return commit(tableId, current -> current.orElse(null));
  }

  /**
   * Applies {@code mutator} to the table's root under CAS, retrying with fresh reads on contention.
   * Returns the committed root (or the untouched current root on a mutator no-op; empty only when
   * the table has no root and the mutator declined to create one).
   *
   * <p>Backward compatibility: when no root is stored yet, the mutator receives a root synthesized
   * from the table's legacy pointer families (its full snapshot history, currency, stats and
   * constraints refs), and the first commit persists that history together with the mutation — a
   * pre-existing deployment migrates lazily, table by table, with no ops step. A pure synthesis
   * (mutator no-op over a synthesized root) is still persisted: materializing the history IS the
   * commit.
   */
  public Optional<TableRoot> commit(ResourceId tableId, RootMutator mutator) {
    BaseResourceRepository.AbortRetryableException lastRetryable = null;
    BaseResourceRepository.NotFoundException lastGone = null;
    for (int attempt = 0; attempt < MAX_COMMIT_ATTEMPTS; attempt++) {
      boolean won;
      TableRoot desired;
      try {
        // THE COMMIT FUNNEL READS LIVE, PERIOD — pointer AND blob. The live pointer read yields
        // the CAS expected-version and names the base root coherently (a cached pointer was a
        // lost-update hazard: a straggling reader could repopulate an older pointer and let this
        // attempt erase an intervening commit). The base BLOB read is live too: its emptiness is
        // the dangling-pointer corruption detector below, which must fire deterministically — a
        // warm decoded root would mask a swept blob, and a CAS retry could flip behavior as the
        // entry evicts. Once per commit, the extra GET is noise on a write path.
        var liveMeta = roots.metaForSafeLive(tableId);
        long expectedVersion = liveMeta.getPointerVersion();
        Optional<TableRoot> stored =
            liveMeta.getBlobUri().isBlank()
                ? Optional.empty()
                : roots.getByBlobUriLive(liveMeta.getBlobUri());
        if (stored.isEmpty() && !liveMeta.getBlobUri().isBlank()) {
          // A pointer exists but its blob is gone. Distinguish the benign supersede+sweep race
          // (the pointer has already moved on — retry re-reads it) from true corruption (pointer
          // unchanged — fail CLOSED). Falling through to synthesis here would fabricate a fresh
          // base over whatever the pointer referenced and mask the data loss behind a misleading
          // CAS-contention exhaustion.
          if (roots.metaForSafeLive(tableId).getPointerVersion() == expectedVersion) {
            throw new BaseResourceRepository.CorruptionException(
                "dangling root pointer, missing blob: " + liveMeta.getBlobUri());
          }
          throw new BaseResourceRepository.AbortRetryableException(
              "root pointer moved mid-read for table " + tableId.getId());
        }
        boolean fromStore = stored.isPresent();
        Optional<TableRoot> current =
            (fromStore || synthesizer == null) ? stored : synthesizer.synthesize(tableId);

        TableRoot produced = mutator.apply(current);
        if (produced == null) {
          if (fromStore || current.isEmpty()) {
            return current; // no-op on a stored root, or nothing exists at all
          }
          produced = current.get(); // no-op mutation, but the synthesized history must persist
        } else if (fromStore && current.get().equals(produced)) {
          return current; // no-op: nothing to commit
        }
        desired =
            produced.toBuilder()
                .setTableId(tableId)
                .setRootSeq(current.map(r -> r.getRootSeq() + 1).orElse(1L))
                .setCommittedAt(Timestamps.fromMillis(System.currentTimeMillis()))
                .build();

        won = fromStore ? roots.update(desired, expectedVersion) : roots.createIfAbsent(desired);
      } catch (BaseResourceRepository.AbortRetryableException retryable) {
        // Transient store contention on a read or the CAS itself: retry with fresh reads.
        lastRetryable = retryable;
        won = false;
        desired = null;
      } catch (BaseResourceRepository.NotFoundException gone) {
        // The root pointer was deleted between our read and the CAS (a racing DROP / account
        // cascade). Honor the retry-merge contract instead of failing terminally: re-read so the
        // next attempt derives from the deleted state ("no root"), and the mutator decides.
        lastGone = gone;
        won = false;
        desired = null;
      } catch (BaseResourceRepository.RepoException terminal) {
        throw new CommitFailedException(
            "table root commit failed for table " + tableId.getId(), terminal);
      }
      if (won) {
        return Optional.of(desired);
      }
      if (attempt < MAX_COMMIT_ATTEMPTS - 1 && !backoff(attempt)) {
        throw new CommitFailedException(
            "table root commit interrupted for table " + tableId.getId());
      }
    }
    String reason;
    Throwable cause;
    if (lastRetryable != null) {
      reason =
          "; a retryable store fault occurred during retries (see cause), not only CAS"
              + " contention";
      cause = lastRetryable;
    } else if (lastGone != null) {
      // Every attempt lost to a racing root-pointer deletion (DROP / account cascade), not to a
      // CAS version race — surface that distinctly so the log doesn't misattribute it to
      // contention.
      reason =
          "; the root pointer was repeatedly deleted mid-commit (racing DROP / account"
              + " cascade), see cause";
      cause = lastGone;
    } else {
      reason = " under CAS contention";
      cause = null;
    }
    throw new CommitFailedException(
        "table root commit exhausted "
            + MAX_COMMIT_ATTEMPTS
            + " attempts for table "
            + tableId.getId()
            + reason,
        cause);
  }

  private static boolean backoff(int attempt) {
    try {
      // Small bounded backoff to de-synchronize contending committers.
      Thread.sleep(Math.min(1L << attempt, 8L));
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }
}

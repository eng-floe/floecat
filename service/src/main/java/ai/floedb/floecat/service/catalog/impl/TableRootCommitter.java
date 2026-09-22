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
import ai.floedb.floecat.service.repo.util.TableBlobReachabilityGuard;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;
import org.eclipse.microprofile.config.inject.ConfigProperty;

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
 *
 * <p>Commit attempts for the same table are serialized inside one service process. This prevents
 * local publishers from defeating one another's CAS attempt under a finalization backlog. The lock
 * is released during contention backoff so one remote CAS loss cannot stall the local table queue;
 * the CAS retry loop remains necessary for writers in other service processes.
 */
@ApplicationScoped
public class TableRootCommitter {

  // Field initializers supply the production defaults to tests that construct this class without
  // ArC; application.properties is the single source of configuration defaults at runtime.
  private static final int DEFAULT_MAX_COMMIT_ATTEMPTS = 8;
  private static final long DEFAULT_COMMIT_BACKOFF_BASE_MS = 100L;
  private static final long DEFAULT_COMMIT_BACKOFF_MAX_MS = 5_000L;
  private static final int COMMIT_LOCK_STRIPES = 4096;

  @ConfigProperty(name = "floecat.table-root-commit.max-attempts")
  int maxCommitAttempts = DEFAULT_MAX_COMMIT_ATTEMPTS;

  @ConfigProperty(name = "floecat.table-root-commit.base-backoff-ms")
  long commitBackoffBaseMs = DEFAULT_COMMIT_BACKOFF_BASE_MS;

  @ConfigProperty(name = "floecat.table-root-commit.max-backoff-ms")
  long commitBackoffMaxMs = DEFAULT_COMMIT_BACKOFF_MAX_MS;

  private final TableRootRepository roots;
  private final TableBlobReachabilityGuard reachabilityGuard;
  private final ReentrantLock[] commitLocks = newCommitLocks();

  @Inject
  public TableRootCommitter(
      TableRootRepository roots, TableBlobReachabilityGuard reachabilityGuard) {
    this.roots = roots;
    this.reachabilityGuard = reachabilityGuard;
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
   * Applies {@code mutator} to the table's root under CAS, retrying with fresh reads on contention.
   * Returns the committed root (or the untouched current root on a mutator no-op; empty only when
   * the table has no root and the mutator declined to create one).
   */
  public Optional<TableRoot> commit(ResourceId tableId, RootMutator mutator) {
    return commitWithRetry(tableId, mutator);
  }

  private ReentrantLock commitLock(ResourceId tableId) {
    int hash = 31 * tableId.getAccountId().hashCode() + tableId.getId().hashCode();
    return commitLocks[Math.floorMod(hash, commitLocks.length)];
  }

  private static ReentrantLock[] newCommitLocks() {
    ReentrantLock[] locks = new ReentrantLock[COMMIT_LOCK_STRIPES];
    for (int index = 0; index < locks.length; index++) {
      locks[index] = new ReentrantLock(true);
    }
    return locks;
  }

  private Optional<TableRoot> commitWithRetry(ResourceId tableId, RootMutator mutator) {
    BaseResourceRepository.AbortRetryableException lastRetryable = null;
    BaseResourceRepository.NotFoundException lastGone = null;
    int attempts = Math.max(1, maxCommitAttempts);
    for (int attempt = 0; attempt < attempts; attempt++) {
      CommitAttempt result = CommitAttempt.retry();
      ReentrantLock lock = commitLock(tableId);
      lock.lock();
      try {
        result = reachabilityGuard.publishing(tableId, () -> commitAttempt(tableId, mutator));
      } catch (BaseResourceRepository.AbortRetryableException retryable) {
        // Transient store contention on a read or the CAS itself: retry with fresh reads.
        lastRetryable = retryable;
      } catch (BaseResourceRepository.NotFoundException gone) {
        // The root pointer was deleted between our read and the CAS (a racing DROP / account
        // cascade). Honor the retry-merge contract instead of failing terminally: re-read so the
        // next attempt derives from the deleted state ("no root"), and the mutator decides.
        lastGone = gone;
      } catch (BaseResourceRepository.AccountDeletionInProgressException deleting) {
        throw deleting;
      } catch (BaseResourceRepository.RepoException terminal) {
        throw new CommitFailedException(
            "table root commit failed for table " + tableId.getId(), terminal);
      } finally {
        lock.unlock();
      }
      if (result.completed()) {
        return result.root();
      }
      // Deliberately outside the table lock: a remote winner must not block local writers while
      // this caller waits to derive its next attempt from the new root.
      if (attempt < attempts - 1 && !backoff(attempt)) {
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
            + attempts
            + " attempts for table "
            + tableId.getId()
            + reason,
        cause);
  }

  private CommitAttempt commitAttempt(ResourceId tableId, RootMutator mutator) {
    // THE COMMIT FUNNEL READS LIVE, PERIOD — pointer AND blob. The live pointer read yields
    // the CAS expected-version and names the base root coherently (a cached pointer was a
    // lost-update hazard: a straggling reader could repopulate an older pointer and let this
    // attempt erase an intervening commit). The base BLOB read is live too: its emptiness is
    // the dangling-pointer corruption detector below, which must fire deterministically — a
    // warm decoded root would mask a swept blob, and a CAS retry could flip behavior as the
    // entry evicts. Once per commit, the extra GET is noise on a write path.
    var liveMeta = roots.metaForSafeConsistent(tableId);
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
      // Compared on the blob uri, not the pointer version: a re-commit of byte-identical
      // content lands on the same content-addressed uri at a NEW version, so a version
      // comparison calls a genuinely dangling pointer "moved" and retries until it exhausts.
      if (liveMeta.getBlobUri().equals(roots.metaForSafeConsistent(tableId).getBlobUri())) {
        // One re-probe first, the rule the other dangling verdicts follow: content is
        // addressed by its bytes, so a revert re-PUTs the very uri that was swept, and a
        // reader landing between the two live reads would otherwise call it corruption.
        if (roots.getByBlobUriLive(liveMeta.getBlobUri()).isPresent()) {
          throw new BaseResourceRepository.AbortRetryableException(
              "root blob reappeared mid-read for table " + tableId.getId());
        }
        throw new BaseResourceRepository.CorruptionException(
            "dangling root pointer, missing blob: " + liveMeta.getBlobUri());
      }
      throw new BaseResourceRepository.AbortRetryableException(
          "root pointer moved mid-read for table " + tableId.getId());
    }
    boolean fromStore = stored.isPresent();
    Optional<TableRoot> current = stored;

    TableRoot produced = mutator.apply(current);
    if (produced == null || (fromStore && current.get().equals(produced))) {
      return CommitAttempt.completed(current);
    }
    TableRoot desired =
        produced.toBuilder()
            .setTableId(tableId)
            .setRootSeq(current.map(r -> r.getRootSeq() + 1).orElse(1L))
            .setCommittedAt(Timestamps.fromMillis(System.currentTimeMillis()))
            .build();
    if (desired.hasSnapshotManifestRef()) {
      roots.requireManifestHeadLive(tableId, desired.getSnapshotManifestRef());
    }

    boolean won =
        fromStore ? roots.update(desired, expectedVersion) : roots.createIfAbsent(desired);
    return won ? CommitAttempt.completed(Optional.of(desired)) : CommitAttempt.retry();
  }

  private record CommitAttempt(boolean completed, Optional<TableRoot> root) {
    private static CommitAttempt completed(Optional<TableRoot> root) {
      return new CommitAttempt(true, root);
    }

    private static CommitAttempt retry() {
      return new CommitAttempt(false, Optional.empty());
    }
  }

  private boolean backoff(int attempt) {
    try {
      long baseMs = Math.max(1L, commitBackoffBaseMs);
      long maxMs = Math.max(baseMs, commitBackoffMaxMs);
      long multiplier = 1L << Math.min(attempt, 30);
      long ceiling = baseMs > maxMs / multiplier ? maxMs : baseMs * multiplier;
      long floor = Math.max(1L, ceiling / 2L);
      long delayMs =
          floor == ceiling ? floor : ThreadLocalRandom.current().nextLong(floor, ceiling);
      Thread.sleep(delayMs);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }
}

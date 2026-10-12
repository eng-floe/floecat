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
import org.jboss.logging.Logger;

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

  private static final Logger LOG = Logger.getLogger(TableRootCommitter.class);

  // Field initializers supply the production defaults to tests that construct this class without
  // ArC; application.properties is the single source of configuration defaults at runtime.
  private static final int DEFAULT_MAX_COMMIT_ATTEMPTS = 8;
  private static final long DEFAULT_COMMIT_BACKOFF_BASE_MS = 100L;
  private static final long DEFAULT_COMMIT_BACKOFF_MAX_MS = 5_000L;
  private static final long DEFAULT_SLOW_LOG_MS = 250L;
  private static final int COMMIT_LOCK_STRIPES = 4096;

  @ConfigProperty(name = "floecat.table-root-commit.max-attempts")
  int maxCommitAttempts = DEFAULT_MAX_COMMIT_ATTEMPTS;

  @ConfigProperty(name = "floecat.table-root-commit.base-backoff-ms")
  long commitBackoffBaseMs = DEFAULT_COMMIT_BACKOFF_BASE_MS;

  @ConfigProperty(name = "floecat.table-root-commit.max-backoff-ms")
  long commitBackoffMaxMs = DEFAULT_COMMIT_BACKOFF_MAX_MS;

  @ConfigProperty(name = "floecat.table-root-commit.slow-log-ms")
  long slowLogMs = DEFAULT_SLOW_LOG_MS;

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
   * Applies the named {@code mutator} to the table's root under CAS, retrying with fresh reads on
   * contention. Returns the committed root (or the untouched current root on a mutator no-op; empty
   * only when the table has no root and the mutator declined to create one). The operation name
   * identifies the mutation in slow-commit diagnostics.
   */
  public Optional<TableRoot> commit(ResourceId tableId, String operation, RootMutator mutator) {
    return commitWithRetry(tableId, operation, mutator);
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

  private Optional<TableRoot> commitWithRetry(
      ResourceId tableId, String operation, RootMutator mutator) {
    BaseResourceRepository.AbortRetryableException lastRetryable = null;
    BaseResourceRepository.NotFoundException lastGone = null;
    int attempts = Math.max(1, maxCommitAttempts);
    long backoffBeforeNanos = 0L;
    for (int attempt = 0; attempt < attempts; attempt++) {
      CommitAttempt result = CommitAttempt.retry();
      ReentrantLock lock = commitLock(tableId);
      int attemptNumber = attempt + 1;
      long attemptStartedNanos = System.nanoTime();
      lock.lock();
      long lockWaitNanos = System.nanoTime() - attemptStartedNanos;
      long attemptBackoffNanos = backoffBeforeNanos;
      try {
        result =
            reachabilityGuard.publishing(
                tableId,
                () ->
                    commitAttempt(
                        tableId,
                        operation,
                        new AttemptTiming(
                            attemptNumber, attemptStartedNanos, lockWaitNanos, attemptBackoffNanos),
                        mutator));
      } catch (BaseResourceRepository.AbortRetryableException retryable) {
        // Transient store contention on a read or the CAS itself: retry with fresh reads.
        logFailedAttempt(
            tableId,
            operation,
            attemptNumber,
            attemptStartedNanos,
            lockWaitNanos,
            attemptBackoffNanos,
            retryable);
        lastRetryable = retryable;
      } catch (BaseResourceRepository.NotFoundException gone) {
        // The root pointer was deleted between our read and the CAS (a racing DROP / account
        // cascade). Honor the retry-merge contract instead of failing terminally: re-read so the
        // next attempt derives from the deleted state ("no root"), and the mutator decides.
        logFailedAttempt(
            tableId,
            operation,
            attemptNumber,
            attemptStartedNanos,
            lockWaitNanos,
            attemptBackoffNanos,
            gone);
        lastGone = gone;
      } catch (BaseResourceRepository.AccountDeletionInProgressException deleting) {
        logFailedAttempt(
            tableId,
            operation,
            attemptNumber,
            attemptStartedNanos,
            lockWaitNanos,
            attemptBackoffNanos,
            deleting);
        throw deleting;
      } catch (BaseResourceRepository.RepoException terminal) {
        logFailedAttempt(
            tableId,
            operation,
            attemptNumber,
            attemptStartedNanos,
            lockWaitNanos,
            attemptBackoffNanos,
            terminal);
        throw new CommitFailedException(
            "table root commit failed for table " + tableId.getId(), terminal);
      } catch (RuntimeException unexpected) {
        logFailedAttempt(
            tableId,
            operation,
            attemptNumber,
            attemptStartedNanos,
            lockWaitNanos,
            attemptBackoffNanos,
            unexpected);
        throw unexpected;
      } finally {
        lock.unlock();
      }
      if (result.completed()) {
        return result.root();
      }
      // Deliberately outside the table lock: a remote winner must not block local writers while
      // this caller waits to derive its next attempt from the new root.
      if (attempt < attempts - 1) {
        long backoffStartedNanos = System.nanoTime();
        if (!backoff(attempt)) {
          throw new CommitFailedException(
              "table root commit interrupted for table " + tableId.getId());
        }
        backoffBeforeNanos = System.nanoTime() - backoffStartedNanos;
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

  private CommitAttempt commitAttempt(
      ResourceId tableId, String operation, AttemptTiming timing, RootMutator mutator) {
    // THE COMMIT FUNNEL READS LIVE, PERIOD — pointer AND blob. The live pointer read yields
    // the CAS expected-version and names the base root coherently (a cached pointer was a
    // lost-update hazard: a straggling reader could repopulate an older pointer and let this
    // attempt erase an intervening commit). The base BLOB read is live too: its emptiness is
    // the dangling-pointer corruption detector below, which must fire deterministically — a
    // warm decoded root would mask a swept blob, and a CAS retry could flip behavior as the
    // entry evicts. Once per commit, the extra GET is noise on a write path.
    long pointerReadStartedNanos = System.nanoTime();
    var liveMeta = roots.metaForSafeConsistent(tableId);
    long pointerReadNanos = System.nanoTime() - pointerReadStartedNanos;
    long expectedVersion = liveMeta.getPointerVersion();
    long rootReadStartedNanos = System.nanoTime();
    Optional<TableRoot> stored =
        liveMeta.getBlobUri().isBlank()
            ? Optional.empty()
            : roots.getByBlobUriLive(liveMeta.getBlobUri());
    long rootReadNanos = System.nanoTime() - rootReadStartedNanos;
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

    long manifestReadsBeforeMutator = roots.liveManifestPageReadCount();
    long mutatorStartedNanos = System.nanoTime();
    TableRoot produced = mutator.apply(current);
    long mutatorNanos = System.nanoTime() - mutatorStartedNanos;
    long mutatorManifestReads = roots.liveManifestPageReadCount() - manifestReadsBeforeMutator;
    if (produced == null || (fromStore && current.get().equals(produced))) {
      logAttempt(
          tableId,
          operation,
          timing,
          "noop",
          pointerReadNanos,
          rootReadNanos,
          mutatorNanos,
          0L,
          0L,
          mutatorManifestReads,
          0L);
      return CommitAttempt.completed(current);
    }
    TableRoot desired =
        produced.toBuilder()
            .setTableId(tableId)
            .setRootSeq(current.map(r -> r.getRootSeq() + 1).orElse(1L))
            .setCommittedAt(Timestamps.fromMillis(System.currentTimeMillis()))
            .build();
    long manifestReadsBeforeValidation = roots.liveManifestPageReadCount();
    long validationStartedNanos = System.nanoTime();
    if (desired.hasSnapshotManifestRef()) {
      roots.requireManifestHeadLive(tableId, desired.getSnapshotManifestRef());
    }
    long validationNanos = System.nanoTime() - validationStartedNanos;
    long validationManifestReads =
        roots.liveManifestPageReadCount() - manifestReadsBeforeValidation;

    long writeStartedNanos = System.nanoTime();
    boolean won =
        fromStore ? roots.update(desired, expectedVersion) : roots.createIfAbsent(desired);
    long writeNanos = System.nanoTime() - writeStartedNanos;
    logAttempt(
        tableId,
        operation,
        timing,
        won ? "committed" : "retry",
        pointerReadNanos,
        rootReadNanos,
        mutatorNanos,
        validationNanos,
        writeNanos,
        mutatorManifestReads,
        validationManifestReads);
    return won ? CommitAttempt.completed(Optional.of(desired)) : CommitAttempt.retry();
  }

  /**
   * Per-attempt diagnostics. {@code mutator_manifest_gets} counts cache-bypassing manifest page
   * GETs; for {@code snapshot_upsert} on a table with a current snapshot it equals the manifest
   * chain's page count, because the upsert and its reusable-candidate rebuild together load every
   * page exactly once through the chain's page cache. {@code backoff_before_ms} is the sleep that
   * preceded this attempt (zero on the first).
   */
  private void logAttempt(
      ResourceId tableId,
      String operation,
      AttemptTiming timing,
      String result,
      long pointerReadNanos,
      long rootReadNanos,
      long mutatorNanos,
      long validationNanos,
      long writeNanos,
      long mutatorManifestReads,
      long validationManifestReads) {
    long totalNanos = System.nanoTime() - timing.startedNanos();
    String format =
        "table_root_commit operation=%s table_id=%s attempt=%d result=%s total_ms=%.1f"
            + " backoff_before_ms=%.1f lock_wait_ms=%.1f pointer_read_ms=%.1f"
            + " root_blob_read_ms=%.1f mutator_ms=%.1f validation_ms=%.1f write_cas_ms=%.1f"
            + " mutator_manifest_gets=%d validation_manifest_gets=%d";
    Object[] args =
        new Object[] {
          operation,
          tableId.getId(),
          timing.attempt(),
          result,
          millis(totalNanos),
          millis(timing.backoffBeforeNanos()),
          millis(timing.lockWaitNanos()),
          millis(pointerReadNanos),
          millis(rootReadNanos),
          millis(mutatorNanos),
          millis(validationNanos),
          millis(writeNanos),
          mutatorManifestReads,
          validationManifestReads
        };
    if (totalNanos >= Math.max(0L, slowLogMs) * 1_000_000L) {
      LOG.infof(format, args);
    } else if (LOG.isDebugEnabled()) {
      LOG.debugf(format, args);
    }
  }

  /**
   * Logs an attempt that threw before reaching a commit outcome. Always INFO: these are rare, and
   * their time is otherwise invisible because the phase timings are lost with the exception.
   */
  private static void logFailedAttempt(
      ResourceId tableId,
      String operation,
      int attempt,
      long startedNanos,
      long lockWaitNanos,
      long backoffBeforeNanos,
      RuntimeException error) {
    LOG.infof(
        "table_root_commit operation=%s table_id=%s attempt=%d result=failed error=%s"
            + " total_ms=%.1f backoff_before_ms=%.1f lock_wait_ms=%.1f",
        operation,
        tableId.getId(),
        attempt,
        error.getClass().getSimpleName(),
        millis(System.nanoTime() - startedNanos),
        millis(backoffBeforeNanos),
        millis(lockWaitNanos));
  }

  private static double millis(long nanos) {
    return nanos / 1_000_000.0;
  }

  private record AttemptTiming(
      int attempt, long startedNanos, long lockWaitNanos, long backoffBeforeNanos) {}

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

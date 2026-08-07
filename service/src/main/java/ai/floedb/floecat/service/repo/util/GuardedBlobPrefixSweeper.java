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

import ai.floedb.floecat.storage.spi.BlobStore;
import java.util.HashSet;

/** Paged, account-lifecycle-fenced deletion of an object-store prefix. */
public final class GuardedBlobPrefixSweeper {
  private static final int PAGE_SIZE = 1_000;

  private GuardedBlobPrefixSweeper() {}

  public static int delete(
      BlobStore blobStore,
      String prefix,
      BatchGuard guard,
      BaseResourceRepository.GuardedDeleteProgress deleteProgress) {
    int deletedTotal = 0;
    var seenTokens = new HashSet<String>();
    String token = "";
    while (true) {
      var page = blobStore.list(prefix, PAGE_SIZE, token);
      int unresolved = 0;
      for (String key : page.keys()) {
        var header = blobStore.head(key).orElse(null);
        if (header == null) {
          // LIST can race another cleanup. HEAD is authoritative for this object and absence lets
          // the scan advance without turning a harmless stale listing into a teardown failure.
          continue;
        }
        requireGuard(guard, deleteProgress);
        boolean deleted;
        if (blobStore.supportsVersionedDeletes() && !header.getVersionId().isBlank()) {
          deleted = blobStore.delete(key, header.getVersionId());
        } else {
          deleted = blobStore.delete(key);
        }
        if (deleted) {
          deletedTotal++;
          deleteProgress.recordWrite();
        } else if (blobStore.head(key).isPresent()) {
          // The exact version observed above was not removed and the key remains live. Advancing
          // the page would silently leave it behind; re-listing the first page would spin forever.
          unresolved++;
        }
      }
      if (unresolved > 0) {
        throw new BaseResourceRepository.AbortRetryableException(
            "blob cleanup could not delete " + unresolved + " object(s) under: " + prefix);
      }

      String next = page.nextToken() == null ? "" : page.nextToken();
      if (next.isBlank()) {
        return deletedTotal;
      }
      if (!seenTokens.add(next)) {
        throw new IllegalStateException("blob scan did not advance; repeated page token: " + next);
      }
      token = next;
    }
  }

  private static void requireGuard(
      BatchGuard guard, BaseResourceRepository.GuardedDeleteProgress deleteProgress) {
    if (guard.reevaluate() == BatchGuard.Outcome.HOLDS) {
      return;
    }
    String message = "blob cleanup lost the race against " + guard.describe();
    if (deleteProgress.hasPriorWrite()) {
      throw new BaseResourceRepository.BatchGuardFailedAfterWriteException(message);
    }
    throw new BaseResourceRepository.BatchGuardFailedException(message);
  }
}

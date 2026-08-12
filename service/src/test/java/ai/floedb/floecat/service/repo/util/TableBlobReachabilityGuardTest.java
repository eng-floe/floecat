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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class TableBlobReachabilityGuardTest {

  @Test
  void exactTableDeleteExcludesItsPublisherButNotUnrelatedTables() throws Exception {
    TableBlobReachabilityGuard guard = new TableBlobReachabilityGuard();
    CountDownLatch deletionStarted = new CountDownLatch(1);
    CountDownLatch finishDeletion = new CountDownLatch(1);
    CountDownLatch sameTablePublished = new CountDownLatch(1);

    try (var executor = Executors.newVirtualThreadPerTaskExecutor();
        var proof = guard.beginProof("account", "table")) {
      var deletion =
          executor.submit(
              () ->
                  guard.deleteIfUnchanged(
                      proof,
                      () -> {
                        deletionStarted.countDown();
                        await(finishDeletion);
                        return true;
                      }));
      assertThat(deletionStarted.await(5, TimeUnit.SECONDS)).isTrue();

      var sameTablePublication =
          executor.submit(
              () ->
                  guard.publishing(
                      "account",
                      "table",
                      () -> {
                        sameTablePublished.countDown();
                        return null;
                      }));
      guard.publishing("account", "unrelated", () -> null);

      assertThat(sameTablePublished.await(100, TimeUnit.MILLISECONDS)).isFalse();
      finishDeletion.countDown();
      assertThat(deletion.get().changed()).isFalse();
      sameTablePublication.get();
      assertThat(sameTablePublished.await(5, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(guard.retainedEntryCount()).isZero();
  }

  @Test
  void publicationInvalidatesAnOlderProof() {
    TableBlobReachabilityGuard guard = new TableBlobReachabilityGuard();

    try (var proof = guard.beginProof("account", "table")) {
      guard.publishing("account", "table", () -> null);

      assertThat(guard.deleteIfUnchanged(proof, () -> true).changed()).isTrue();
    }

    assertThat(guard.retainedEntryCount()).isZero();
  }

  @Test
  void sameTablePublishersCanRunConcurrently() throws Exception {
    TableBlobReachabilityGuard guard = new TableBlobReachabilityGuard();
    CountDownLatch publicationsStarted = new CountDownLatch(2);
    CountDownLatch finishPublications = new CountDownLatch(1);

    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var first =
          executor.submit(
              () ->
                  guard.publishing(
                      "account",
                      "table",
                      () -> {
                        publicationsStarted.countDown();
                        await(finishPublications);
                        return null;
                      }));
      var second =
          executor.submit(
              () ->
                  guard.publishing(
                      "account",
                      "table",
                      () -> {
                        publicationsStarted.countDown();
                        await(finishPublications);
                        return null;
                      }));

      assertThat(publicationsStarted.await(5, TimeUnit.SECONDS)).isTrue();
      finishPublications.countDown();
      first.get();
      second.get();
    }

    assertThat(guard.retainedEntryCount()).isZero();
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while coordinating test", e);
    }
  }
}

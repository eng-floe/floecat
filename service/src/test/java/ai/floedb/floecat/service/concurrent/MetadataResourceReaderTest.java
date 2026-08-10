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
package ai.floedb.floecat.service.concurrent;

import static ai.floedb.floecat.service.testsupport.ConcurrentTestSupport.awaitUninterruptibly;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.service.context.PropagatedContext;
import ai.floedb.floecat.service.repo.util.RepositoryReads;
import ai.floedb.floecat.service.testsupport.ConcurrentTestSupport;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** Verifies leaf storage adapters apply admission and propagated cancellation. */
class MetadataResourceReaderTest {

  @Test
  void backendReadRunsOnAnAdmittedVirtualThread() throws Exception {
    ReaderFixture fixture = readerFixture();
    when(fixture.blobs().get("blob"))
        .thenAnswer(
            ignored -> {
              assertThat(fixture.runner().permitsInUse()).isEqualTo(1);
              assertThat(Thread.currentThread().isVirtual()).isTrue();
              return new byte[] {1, 2, 3};
            });

    assertThat(fixture.reads().blobs().get("blob")).containsExactly(1, 2, 3);
    ConcurrentTestSupport.await(() -> fixture.runner().permitsInUse() == 0, Duration.ofSeconds(2));
  }

  @Test
  void alreadyCancelledRequestDoesNotReachTheBackend() {
    ReaderFixture fixture = readerFixture();

    try (PropagatedContext.CancellationScope ignored =
        PropagatedContext.bindCancellation(() -> true)) {
      assertThatThrownBy(() -> fixture.reads().blobs().get("blob"))
          .isInstanceOf(CancellationException.class);
    }
    verify(fixture.blobs(), never()).get("blob");
  }

  @Test
  void liveCancellationAbandonsTheAdapterCall() throws Exception {
    ReaderFixture fixture = readerFixture();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    when(fixture.blobs().get("blob"))
        .thenAnswer(
            ignored -> {
              entered.countDown();
              awaitUninterruptibly(release);
              return new byte[] {1};
            });
    AtomicBoolean cancelled = new AtomicBoolean();
    CompletableFuture<byte[]> call =
        CompletableFuture.supplyAsync(
            () -> {
              try (PropagatedContext.CancellationScope ignored =
                  PropagatedContext.bindCancellation(cancelled::get)) {
                return fixture.reads().blobs().get("blob");
              }
            });
    assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();

    cancelled.set(true);
    assertThatThrownBy(() -> call.get(2, TimeUnit.SECONDS))
        .hasCauseInstanceOf(CancellationException.class);
    assertThat(fixture.runner().permitsInUse()).isEqualTo(1);

    release.countDown();
    ConcurrentTestSupport.await(() -> fixture.runner().permitsInUse() == 0, Duration.ofSeconds(2));
    verify(fixture.blobs()).get("blob");
  }

  /** Build the isolated runner, mocked blob store, and admitted adapters shared by each test. */
  private static ReaderFixture readerFixture() {
    MetadataIoRunner runner = new MetadataIoRunner(1);
    BlobStore blobs = mock(BlobStore.class);
    RepositoryReads reads =
        RepositoryReads.bind(mock(PointerStore.class), blobs, new MetadataResourceReader(runner));
    return new ReaderFixture(runner, blobs, reads);
  }

  /** Collaborators composing one isolated admitted-reader test fixture. */
  private record ReaderFixture(MetadataIoRunner runner, BlobStore blobs, RepositoryReads reads) {}
}

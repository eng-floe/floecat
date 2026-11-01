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

package ai.floedb.floecat.types;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

class RelationFailuresTest {

  private static final class StoreDown extends RuntimeException implements InfrastructureFailure {}

  @Test
  void infrastructureAndControlFlowFailTheCallAndEverythingElseIsTheRelations() {
    List<Throwable> infrastructure =
        List.of(
            new StoreDown(),
            new CompletionException(new StoreDown()),
            new IllegalStateException("decode failed", new StoreDown()),
            new CancellationException(),
            new ExecutionException(new TimeoutException()),
            new CompletionException(new InterruptedException()),
            new RejectedExecutionException(),
            new OutOfMemoryError(),
            Status.UNAVAILABLE.asRuntimeException(),
            Status.DEADLINE_EXCEEDED.asRuntimeException(),
            Status.CANCELLED.asRuntimeException(),
            Status.ABORTED.asRuntimeException(),
            Status.RESOURCE_EXHAUSTED.asRuntimeException(),
            Status.UNAUTHENTICATED.asException(),
            Status.DATA_LOSS.asRuntimeException());
    List<Throwable> relations =
        List.of(
            new IllegalStateException("mapper invariant broken"),
            new CompletionException(new IllegalArgumentException("bad type")),
            Status.NOT_FOUND.asRuntimeException(),
            Status.PERMISSION_DENIED.asRuntimeException(),
            Status.INVALID_ARGUMENT.asRuntimeException(),
            Status.FAILED_PRECONDITION.asRuntimeException(),
            Status.INTERNAL.asRuntimeException());

    assertThat(infrastructure).filteredOn(f -> !RelationFailures.isInfrastructure(f)).isEmpty();
    assertThat(relations).filteredOn(RelationFailures::isInfrastructure).isEmpty();
  }

  @Test
  void isolateRethrowsInfrastructureAndWarnsOncePerRelationWhileTheLimiterHasRoom() {
    RelationFailures.LAST_WARNED.clear();
    List<LogRecord> records = new ArrayList<>();
    Handler capture =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            records.add(record);
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    Logger log = Logger.getLogger(RelationFailures.class.getName());
    Level level = log.getLevel();
    log.setLevel(Level.ALL);
    log.addHandler(capture);
    try {
      var storeDown = new StoreDown();
      assertThatThrownBy(() -> RelationFailures.isolate(table("orders"), storeDown))
          .isSameAs(storeDown);

      RelationFailures.isolate(table("customers"), new IllegalStateException("first"));
      RelationFailures.isolate(table("customers"), new IllegalStateException("second"));
      RelationFailures.isolate(
          RelationFailures.label(
              "acct",
              NameRef.newBuilder().setCatalog("cat").addPath("sales").setName("items").build()),
          new IllegalStateException("third"));
      RelationFailures.isolate(table("missing"), Status.NOT_FOUND.asRuntimeException());

      long now = System.nanoTime();
      fillLimiter(now - TimeUnit.MINUTES.toNanos(6));
      RelationFailures.isolate(table("evicted"), new IllegalStateException("stale entries"));
      fillLimiter(now);
      RelationFailures.isolate(table("crowded"), new IllegalStateException("limiter full"));
    } finally {
      log.removeHandler(capture);
      log.setLevel(level);
      RelationFailures.LAST_WARNED.clear();
    }

    assertThat(records)
        .extracting(LogRecord::getLevel, LogRecord::getMessage, r -> r.getThrown().getMessage())
        .containsExactly(
            tuple(
                Level.WARNING,
                "Relation failed on its own, account=acct RK_TABLE=customers",
                "first"),
            tuple(
                Level.WARNING,
                "Relation failed on its own, account=acct relation=cat.sales.items",
                "third"),
            tuple(
                Level.FINE,
                "Relation failed on its own, account=acct RK_TABLE=missing",
                "NOT_FOUND"),
            tuple(
                Level.WARNING,
                "Relation failed on its own, account=acct RK_TABLE=evicted",
                "stale entries"),
            tuple(
                Level.FINE,
                "Relation failed on its own, account=acct RK_TABLE=crowded",
                "limiter full"));
  }

  private static void fillLimiter(long warnedAt) {
    RelationFailures.LAST_WARNED.clear();
    RelationFailures.nextSweepNanos = warnedAt;
    for (int i = 0; i < RelationFailures.MAX_WARNED_RELATIONS; i++) {
      RelationFailures.LAST_WARNED.put("filler-" + i, warnedAt);
    }
  }

  private static String table(String id) {
    return RelationFailures.label(
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId(id)
            .build());
  }
}

/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package ai.floedb.floecat.service.account;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Deployment-neutral lifecycle contract used by the pod drain endpoint.
 *
 * <p>OSS Floecat supplies a standalone implementation. A deployment may replace this bean with a
 * runtime-specific implementation, but the query, cache, mutation, and GC code only observes the
 * admission and GC permits exposed by {@link AccountScope}.
 */
public interface LifecycleDrain extends LifecycleControl {
  /** Fallback used by plain unit-test instances created without CDI. */
  LifecycleDrain ALWAYS_SERVING =
      new LifecycleDrain() {
        private final Status status = new Status("test", "test", false, java.util.List.of(), 0L);
        private final CompletionStage<Void> drained = CompletableFuture.completedFuture(null);

        @Override
        public Permit admitRpc() {
          return () -> {};
        }

        @Override
        public Status beginProcessDrain() {
          return status;
        }

        @Override
        public Status status() {
          return status;
        }

        @Override
        public CompletionStage<Void> drained() {
          return drained;
        }
      };

  Permit admitRpc();

  /**
   * Completes once the process has drained all work admitted before the drain began.
   *
   * <p>The completed default keeps the lifecycle interface source-compatible for standalone users
   * that only need the admission hook; managed implementations override it when the drain endpoint
   * should wait for in-flight work.
   */
  default CompletionStage<Void> drained() {
    return CompletableFuture.completedFuture(null);
  }

  interface Permit extends AutoCloseable {
    @Override
    void close();
  }

  final class DrainingException extends RuntimeException {
    public DrainingException() {
      super("Floecat is draining");
    }
  }
}

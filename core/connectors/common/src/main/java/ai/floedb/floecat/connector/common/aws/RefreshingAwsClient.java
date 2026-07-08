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

package ai.floedb.floecat.connector.common.aws;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

public final class RefreshingAwsClient<T extends AutoCloseable> implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(RefreshingAwsClient.class);
  private static final String CONNECTION_POOL_SHUT_DOWN = "Connection pool shut down";

  private final Supplier<T> factory;
  private final AtomicReference<T> current = new AtomicReference<>();

  public RefreshingAwsClient(Supplier<T> factory) {
    if (factory == null) {
      throw new IllegalArgumentException("factory must not be null");
    }
    this.factory = factory;
  }

  public <R> R call(ThrowingOperation<T, R> operation) throws IOException {
    for (int attempt = 0; ; attempt++) {
      T client = current();
      try {
        return operation.call(client);
      } catch (RuntimeException e) {
        if (attempt == 0 && isConnectionPoolShutdown(e)) {
          LOG.warnf(
              e, "AWS client connection pool shutdown in %s; refreshing client", callerContext());
          refreshAfterFailure(client);
          continue;
        }
        throw e;
      }
    }
  }

  public <R> R callUnchecked(Operation<T, R> operation) {
    for (int attempt = 0; ; attempt++) {
      T client = current();
      try {
        return operation.call(client);
      } catch (RuntimeException e) {
        if (attempt == 0 && isConnectionPoolShutdown(e)) {
          LOG.warnf(
              e, "AWS client connection pool shutdown in %s; refreshing client", callerContext());
          refreshAfterFailure(client);
          continue;
        }
        throw e;
      }
    }
  }

  public T current() {
    T existing = current.get();
    if (existing != null) {
      return existing;
    }
    T next = factory.get();
    if (current.compareAndSet(null, next)) {
      return next;
    }
    closeQuietly(next);
    T winner = current.get();
    if (winner == null) {
      throw new IllegalStateException("AWS client was not initialized");
    }
    return winner;
  }

  public static boolean isConnectionPoolShutdown(Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      String message = current.getMessage();
      if (message != null && message.contains(CONNECTION_POOL_SHUT_DOWN)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private void refreshAfterFailure(T failedClient) {
    if (failedClient == null || current.get() != failedClient) {
      return;
    }
    T next = factory.get();
    if (current.compareAndSet(failedClient, next)) {
      closeQuietly(failedClient);
    } else {
      closeQuietly(next);
    }
  }

  @Override
  public void close() {
    closeQuietly(current.getAndSet(null));
  }

  private static void closeQuietly(AutoCloseable client) {
    if (client == null) {
      return;
    }
    try {
      client.close();
    } catch (Exception ignored) {
    }
  }

  private static String callerContext() {
    String wrapperClassName = RefreshingAwsClient.class.getName();
    for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
      String className = frame.getClassName();
      if (className.equals(Thread.class.getName()) || className.equals(wrapperClassName)) {
        continue;
      }
      if (className.startsWith(wrapperClassName + "$")) {
        continue;
      }
      return className + "." + frame.getMethodName();
    }
    return "unknown";
  }

  @FunctionalInterface
  public interface ThrowingOperation<T, R> {
    R call(T client) throws IOException;
  }

  @FunctionalInterface
  public interface Operation<T, R> {
    R call(T client);
  }
}

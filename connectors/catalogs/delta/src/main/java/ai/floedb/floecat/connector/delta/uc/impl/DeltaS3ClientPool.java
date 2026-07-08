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

package ai.floedb.floecat.connector.delta.uc.impl;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.s3.S3Client;

final class DeltaS3ClientPool {

  private static final Logger LOG = Logger.getLogger(DeltaS3ClientPool.class);
  private static final String CONNECTION_POOL_SHUT_DOWN = "Connection pool shut down";

  private final Supplier<S3Client> factory;
  private final AtomicReference<S3Client> current = new AtomicReference<>();

  DeltaS3ClientPool(Supplier<S3Client> factory) {
    this.factory = factory;
  }

  <T> T call(ThrowingS3Operation<T> operation) throws IOException {
    for (int attempt = 0; ; attempt++) {
      S3Client client = current();
      try {
        return operation.call(client);
      } catch (RuntimeException e) {
        if (attempt == 0 && isConnectionPoolShutdown(e)) {
          LOG.warnf(
              e, "Delta S3 connection pool shutdown in %s; refreshing client", callerContext());
          refreshAfterFailure(client, e);
          continue;
        }
        throw e;
      }
    }
  }

  <T> T callUnchecked(S3Operation<T> operation) {
    for (int attempt = 0; ; attempt++) {
      S3Client client = current();
      try {
        return operation.call(client);
      } catch (RuntimeException e) {
        if (attempt == 0 && isConnectionPoolShutdown(e)) {
          LOG.warnf(
              e, "Delta S3 connection pool shutdown in %s; refreshing client", callerContext());
          refreshAfterFailure(client, e);
          continue;
        }
        throw e;
      }
    }
  }

  private S3Client current() {
    S3Client existing = current.get();
    if (existing != null) {
      return existing;
    }
    S3Client next = factory.get();
    if (current.compareAndSet(null, next)) {
      return next;
    }
    closeQuietly(next);
    S3Client winner = current.get();
    if (winner == null) {
      throw new IllegalStateException("Delta S3 client was not initialized");
    }
    return winner;
  }

  private void refreshAfterFailure(S3Client failedClient, Throwable failure) {
    if (failedClient == null || current.get() != failedClient) {
      return;
    }
    S3Client next = factory.get();
    if (current.compareAndSet(failedClient, next)) {
      closeQuietly(failedClient);
      LOG.warnf(failure, "Refreshed Delta S3 client after closed connection pool");
    } else {
      closeQuietly(next);
    }
  }

  static boolean isConnectionPoolShutdown(Throwable failure) {
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

  private static void closeQuietly(S3Client client) {
    if (client == null) {
      return;
    }
    try {
      client.close();
    } catch (RuntimeException ignored) {
    }
  }

  private static String callerContext() {
    String wrapperClassName = DeltaS3ClientPool.class.getName();
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
  interface ThrowingS3Operation<T> {
    T call(S3Client client) throws IOException;
  }

  @FunctionalInterface
  interface S3Operation<T> {
    T call(S3Client client);
  }
}

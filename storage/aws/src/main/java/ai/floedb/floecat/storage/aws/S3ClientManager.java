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

package ai.floedb.floecat.storage.aws;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.s3.S3Client;

@ApplicationScoped
public class S3ClientManager {

  private static final Logger LOG = Logger.getLogger(S3ClientManager.class);

  @Inject AwsClients awsClients;

  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicReference<S3Client> current = new AtomicReference<>();

  public S3Client current() {
    requireOpen();
    S3Client existing = current.get();
    if (existing != null) {
      return existing;
    }

    S3Client next = awsClients.newS3Client();
    if (closed.get()) {
      next.close();
      requireOpen();
    }
    if (current.compareAndSet(null, next)) {
      if (closed.get() && current.compareAndSet(next, null)) {
        next.close();
        requireOpen();
      }
      return next;
    }

    next.close();
    return requireOpen(current.get());
  }

  public void refreshAfterFailure(S3Client failedClient, Throwable failure) {
    if (!ClosedAwsClientDetector.isConnectionPoolShutdown(failure)) {
      return;
    }
    if (closed.get()) {
      return;
    }
    if (failedClient == null || current.get() != failedClient) {
      return;
    }

    S3Client next = awsClients.newS3Client();
    if (closed.get()) {
      next.close();
      return;
    }
    if (current.compareAndSet(failedClient, next)) {
      closeQuietly(failedClient);
      LOG.warn("Refreshed S3 client after closed connection pool");
    } else {
      next.close();
    }
  }

  @PreDestroy
  void close() {
    if (closed.compareAndSet(false, true)) {
      closeQuietly(current.getAndSet(null));
    }
  }

  private void requireOpen() {
    if (closed.get()) {
      throw new IllegalStateException("s3 client manager is shut down");
    }
  }

  private S3Client requireOpen(S3Client client) {
    requireOpen();
    if (client == null) {
      throw new IllegalStateException("s3 client manager is shut down");
    }
    return client;
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
}

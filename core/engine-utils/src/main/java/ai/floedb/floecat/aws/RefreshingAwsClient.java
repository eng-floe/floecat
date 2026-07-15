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

package ai.floedb.floecat.aws;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class RefreshingAwsClient<T extends AutoCloseable> implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(RefreshingAwsClient.class.getName());

  private final String name;
  private final Supplier<ClientResource<T>> factory;
  private final Consumer<Throwable> beforeRefresh;
  private final Object refreshLock;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicReference<ClientResource<T>> current = new AtomicReference<>();

  public RefreshingAwsClient(Supplier<T> factory) {
    this("AWS", factory);
  }

  public RefreshingAwsClient(String name, Supplier<T> factory) {
    if (factory == null) {
      throw new IllegalArgumentException("factory must not be null");
    }
    this.name = name == null || name.isBlank() ? "AWS" : name;
    this.factory = () -> clientResource(factory.get());
    this.beforeRefresh = failure -> {};
    this.refreshLock = new Object();
  }

  private RefreshingAwsClient(
      String name,
      Supplier<ClientResource<T>> factory,
      Consumer<Throwable> beforeRefresh,
      Object refreshLock) {
    if (factory == null) {
      throw new IllegalArgumentException("factory must not be null");
    }
    this.name = name == null || name.isBlank() ? "AWS" : name;
    this.factory = factory;
    this.beforeRefresh = beforeRefresh == null ? failure -> {} : beforeRefresh;
    this.refreshLock = refreshLock == null ? new Object() : refreshLock;
  }

  public static <T extends AutoCloseable> RefreshingAwsClient<T> withResourceFactory(
      Supplier<ClientResource<T>> factory) {
    return new RefreshingAwsClient<>("AWS", factory, null, null);
  }

  public static <T extends AutoCloseable> RefreshingAwsClient<T> withResourceFactory(
      String name, Supplier<ClientResource<T>> factory) {
    return new RefreshingAwsClient<>(name, factory, null, null);
  }

  public static <T extends AutoCloseable> RefreshingAwsClient<T> withResourceFactory(
      String name, Supplier<ClientResource<T>> factory, Consumer<Throwable> beforeRefresh) {
    return new RefreshingAwsClient<>(name, factory, beforeRefresh, null);
  }

  public static <T extends AutoCloseable> RefreshingAwsClient<T> withResourceFactory(
      String name,
      Supplier<ClientResource<T>> factory,
      Consumer<Throwable> beforeRefresh,
      Object refreshLock) {
    return new RefreshingAwsClient<>(name, factory, beforeRefresh, refreshLock);
  }

  public static <T extends AutoCloseable> ClientResource<T> clientResource(
      T client, AutoCloseable... resources) {
    return new ClientResource<>(client, resources);
  }

  public static AutoCloseable closeableResource(Object resource) {
    return resource instanceof AutoCloseable closeable ? closeable : null;
  }

  public <R, E extends Exception> R call(CheckedOperation<T, R, E> operation) throws E {
    ClientResource<T> resource = currentResource();
    try {
      return operation.call(resource.client());
    } catch (RuntimeException e) {
      if (ClosedAwsClientDetector.isConnectionPoolShutdown(e)) {
        warn(e);
        refreshAfterFailure(resource, e);
        return operation.call(currentResource().client());
      }
      throw e;
    } catch (Exception e) {
      if (ClosedAwsClientDetector.isConnectionPoolShutdown(e)) {
        warn(e);
        refreshAfterFailure(resource, e);
        return operation.call(currentResource().client());
      }
      throw checked(e);
    }
  }

  public <R> R callUnchecked(Function<T, R> operation) {
    ClientResource<T> resource = currentResource();
    try {
      return operation.apply(resource.client());
    } catch (RuntimeException e) {
      if (ClosedAwsClientDetector.isConnectionPoolShutdown(e)) {
        warn(e);
        refreshAfterFailure(resource, e);
        return operation.apply(currentResource().client());
      }
      throw e;
    }
  }

  public <R> CompletableFuture<R> callAsync(Function<T, ? extends CompletionStage<R>> operation) {
    ClientResource<T> resource = currentResource();
    CompletionStage<R> first;
    CancellableCompletableFuture<R> result = new CancellableCompletableFuture<>();
    try {
      first = operation.apply(resource.client());
      result.setActive(first);
    } catch (RuntimeException e) {
      if (!ClosedAwsClientDetector.isConnectionPoolShutdown(e)) {
        throw e;
      }
      warn(e);
      refreshAfterFailure(resource, e);
      applyAsync(operation, currentResource(), result);
      return result;
    }

    first.whenComplete(
        (value, failure) -> {
          if (result.isCancelled()) {
            return;
          }
          if (failure == null) {
            result.complete(value);
            return;
          }
          Throwable unwrapped = ClosedAwsClientDetector.unwrapStageFailure(failure);
          if (!ClosedAwsClientDetector.isConnectionPoolShutdown(unwrapped)) {
            result.completeExceptionally(unwrapped);
            return;
          }
          warn(unwrapped);
          try {
            refreshAfterFailure(resource, unwrapped);
            applyAsync(operation, currentResource(), result);
          } catch (Throwable retryFailure) {
            result.completeExceptionally(ClosedAwsClientDetector.unwrapStageFailure(retryFailure));
          }
        });
    return result;
  }

  public T current() {
    return currentResource().client();
  }

  public void invalidate() {
    closeQuietly(current.getAndSet(null));
  }

  public static boolean isConnectionPoolShutdown(Throwable failure) {
    return ClosedAwsClientDetector.isConnectionPoolShutdown(failure);
  }

  private <R> void applyAsync(
      Function<T, ? extends CompletionStage<R>> operation,
      ClientResource<T> resource,
      CancellableCompletableFuture<R> result) {
    try {
      CompletionStage<R> stage = operation.apply(resource.client());
      result.setActive(stage);
      stage.whenComplete(copyTo(result));
    } catch (RuntimeException e) {
      result.completeExceptionally(e);
    }
  }

  private static <R> java.util.function.BiConsumer<R, Throwable> copyTo(CompletableFuture<R> dest) {
    return (value, failure) -> {
      if (failure == null) {
        dest.complete(value);
      } else {
        dest.completeExceptionally(ClosedAwsClientDetector.unwrapStageFailure(failure));
      }
    };
  }

  private ClientResource<T> currentResource() {
    requireOpen();
    ClientResource<T> existing = current.get();
    if (existing != null) {
      return existing;
    }
    ClientResource<T> next = factory.get();
    if (closed.get()) {
      closeQuietly(next);
      requireOpen();
    }
    if (current.compareAndSet(null, next)) {
      if (closed.get()) {
        if (current.compareAndSet(next, null)) {
          closeQuietly(next);
        }
        requireOpen();
      }
      return next;
    }
    closeQuietly(next);
    ClientResource<T> winner = current.get();
    if (winner == null) {
      requireOpen();
      throw new IllegalStateException(name + " client was not initialized");
    }
    return winner;
  }

  private void refreshAfterFailure(ClientResource<T> failedResource, Throwable failure) {
    synchronized (refreshLock) {
      if (failedResource == null || closed.get() || current.get() != failedResource) {
        return;
      }
      beforeRefresh.accept(failure);
      if (closed.get() || current.get() != failedResource) {
        return;
      }
      ClientResource<T> next = factory.get();
      if (closed.get()) {
        closeQuietly(next);
        return;
      }
      if (current.compareAndSet(failedResource, next)) {
        closeQuietly(failedResource);
      } else {
        closeQuietly(next);
      }
    }
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      closeQuietly(current.getAndSet(null));
    }
  }

  public static void closeQuietly(AutoCloseable client) {
    if (client == null) {
      return;
    }
    try {
      client.close();
    } catch (Exception ignored) {
    }
  }

  private void requireOpen() {
    if (closed.get()) {
      throw new IllegalStateException(name + " client is shut down");
    }
  }

  private void warn(Throwable failure) {
    LOG.log(Level.WARNING, name + " connection pool shutdown; refreshing client", failure);
  }

  @SuppressWarnings("unchecked")
  private static <E extends Exception> E checked(Exception e) throws E {
    throw (E) e;
  }

  public static final class ClientResource<T extends AutoCloseable> implements AutoCloseable {
    private final T client;
    private final List<AutoCloseable> resources;

    private ClientResource(T client, AutoCloseable... resources) {
      if (client == null) {
        throw new IllegalArgumentException("client must not be null");
      }
      this.client = client;
      List<AutoCloseable> closeables = new ArrayList<>();
      if (resources != null) {
        for (AutoCloseable resource : resources) {
          if (resource != null) {
            closeables.add(resource);
          }
        }
      }
      this.resources = List.copyOf(closeables);
    }

    public T client() {
      return client;
    }

    @Override
    public void close() throws Exception {
      Exception failure = null;
      try {
        client.close();
      } catch (Exception e) {
        failure = e;
      }
      for (AutoCloseable resource : resources) {
        try {
          resource.close();
        } catch (Exception e) {
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      }
      if (failure != null) {
        throw failure;
      }
    }
  }

  @FunctionalInterface
  public interface CheckedOperation<T, R, E extends Exception> {
    R call(T client) throws E;
  }

  private static final class CancellableCompletableFuture<R> extends CompletableFuture<R> {
    private final AtomicReference<CompletableFuture<?>> active = new AtomicReference<>();

    private void setActive(CompletionStage<?> stage) {
      if (stage instanceof CompletableFuture<?> future) {
        active.set(future);
        if (isCancelled()) {
          future.cancel(true);
        }
      }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      CompletableFuture<?> future = active.get();
      if (future != null) {
        future.cancel(mayInterruptIfRunning);
      }
      return super.cancel(mayInterruptIfRunning);
    }
  }
}

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

package ai.floedb.floecat.cache;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

/**
 * The shared read-through concurrency protocol used by cache implementations.
 *
 * <p>A load has an owner and an epoch fence. Readers arriving while it is current join its future.
 * Mutations retire the owner before changing the resident value. A retired owner may still finish
 * for callers that already joined it, but {@link #publishIfCurrent(Token, Runnable)} prevents it
 * from restoring an old value. The registration gate closes the snapshot gap during partition
 * eviction, so a caller after the eviction cannot join an owner that the eviction did not see.
 *
 * <p>This class owns only load ownership and ordering. The resident medium remains with the cache
 * adapter: Caffeine, a disk file, or an authoritative pointer index. That keeps one concurrency
 * model without pretending those media have the same lifecycle.
 */
public final class LoadCoordinator<K, V> {

  private static final int FENCE_STRIPES = 256;

  private final ConcurrentMap<K, Slot<V>> loads = new ConcurrentHashMap<>();
  private final ReentrantReadWriteLock registration = new ReentrantReadWriteLock(true);
  private final StampedLock[] fences = new StampedLock[FENCE_STRIPES];
  private final ToIntFunction<K> stripeFunction;

  /** Creates a coordinator; the function selects the mutation-fence stripe for a key. */
  public LoadCoordinator(ToIntFunction<K> stripeFunction) {
    this.stripeFunction = Objects.requireNonNull(stripeFunction, "stripeFunction");
    for (int stripe = 0; stripe < fences.length; stripe++) {
      fences[stripe] = new StampedLock();
    }
  }

  /** Samples the key's fence before the resident probe. */
  public Sample sample(K key) {
    StampedLock fence = fenceFor(key);
    return new Sample(fence, fence.tryOptimisticRead());
  }

  /** Acquires a current owner, or joins the owner that was visible at registration. */
  public Acquisition<K, V> acquire(K key, Sample sample) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(sample, "sample");
    registration.readLock().lock();
    try {
      StampedLock fence = fenceFor(key);
      long stamp =
          sample.fence == fence && fence.validate(sample.stamp)
              ? sample.stamp
              : fence.tryOptimisticRead();
      Slot<V> mine = new Slot<>(Thread.currentThread());
      Slot<V> existing = loads.putIfAbsent(key, mine);
      if (existing != null && existing.owner() == Thread.currentThread()) {
        throw new IllegalStateException("recursive cache load for key " + key);
      }
      Slot<V> selected = existing == null ? mine : existing;
      return new Acquisition<>(new Token<>(key, selected, fence, stamp), existing == null);
    } finally {
      registration.readLock().unlock();
    }
  }

  /** Runs a publication while the key's mutation fence cannot retire it underneath the write. */
  public boolean publishIfCurrent(Token<K, V> token, Runnable publication) {
    Objects.requireNonNull(token, "token");
    Objects.requireNonNull(publication, "publication");
    requireOwner(token);
    long readStamp = token.fence().readLock();
    try {
      if (loads.get(token.key()) != token.slot() || !token.fence().validate(token.stamp())) {
        return false;
      }
      publication.run();
      return true;
    } finally {
      token.fence().unlockRead(readStamp);
    }
  }

  /** Retires a key and applies its mutation atomically with respect to publication. */
  public void mutate(K key, Runnable mutation) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(mutation, "mutation");
    registration.writeLock().lock();
    StampedLock fence = fenceFor(key);
    long stamp = fence.writeLock();
    try {
      loads.remove(key);
      mutation.run();
    } finally {
      fence.unlockWrite(stamp);
      registration.writeLock().unlock();
    }
  }

  /** Retires every matching owner and runs the partition mutation under all fences. */
  public void mutatePartition(Predicate<K> belongsToPartition, Runnable mutation) {
    Objects.requireNonNull(belongsToPartition, "belongsToPartition");
    Objects.requireNonNull(mutation, "mutation");
    registration.writeLock().lock();
    long[] stamps = new long[fences.length];
    try {
      for (int stripe = 0; stripe < fences.length; stripe++) {
        stamps[stripe] = fences[stripe].writeLock();
      }
      loads.keySet().removeIf(belongsToPartition);
      mutation.run();
    } finally {
      for (int stripe = fences.length - 1; stripe >= 0; stripe--) {
        fences[stripe].unlockWrite(stamps[stripe]);
      }
      registration.writeLock().unlock();
    }
  }

  /** Completes a load after removing its ownership, allowing the next miss to start a new epoch. */
  public void complete(Token<K, V> token, V value) {
    requireOwner(token);
    loads.remove(token.key(), token.slot());
    token.slot().future().complete(value);
  }

  /** Completes a load exceptionally after removing its ownership. */
  public void fail(Token<K, V> token, Throwable failure) {
    requireOwner(token);
    loads.remove(token.key(), token.slot());
    token.slot().future().completeExceptionally(failure);
  }

  public V await(Token<K, V> token) {
    if (token.slot().owner() == Thread.currentThread()) {
      throw new IllegalStateException("recursive cache load for key " + token.key());
    }
    try {
      return token.slot().future().join();
    } catch (CompletionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtime) {
        throw runtime;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw e;
    }
  }

  int stripeFor(K key) {
    int spread = stripeFunction.applyAsInt(key) * 0x9E3779B9;
    return (spread >>> 16) & (FENCE_STRIPES - 1);
  }

  private StampedLock fenceFor(K key) {
    return fences[stripeFor(key)];
  }

  private void requireOwner(Token<K, V> token) {
    if (token.slot().owner() != Thread.currentThread()) {
      throw new IllegalStateException("only the cache load owner may publish or complete");
    }
  }

  /** Opaque fence sample passed from the resident probe to {@link #acquire(Object, Sample)}. */
  public static final class Sample {
    private final StampedLock fence;
    private final long stamp;

    private Sample(StampedLock fence, long stamp) {
      this.fence = fence;
      this.stamp = stamp;
    }
  }

  /** The caller's ownership decision and opaque slot token. */
  public static final class Acquisition<K, V> {
    private final Token<K, V> token;
    private final boolean owner;

    private Acquisition(Token<K, V> token, boolean owner) {
      this.token = token;
      this.owner = owner;
    }

    public Token<K, V> token() {
      return token;
    }

    public boolean owner() {
      return owner;
    }
  }

  /** Opaque token used to publish or complete one owned load. */
  public static final class Token<K, V> {
    private final K key;
    private final Slot<V> slot;
    private final StampedLock fence;
    private final long stamp;

    private Token(K key, Slot<V> slot, StampedLock fence, long stamp) {
      this.key = key;
      this.slot = slot;
      this.fence = fence;
      this.stamp = stamp;
    }

    private K key() {
      return key;
    }

    private Slot<V> slot() {
      return slot;
    }

    private StampedLock fence() {
      return fence;
    }

    private long stamp() {
      return stamp;
    }
  }

  private record Slot<V>(Thread owner, CompletableFuture<V> future) {
    private Slot(Thread owner) {
      this(owner, new CompletableFuture<>());
    }
  }
}

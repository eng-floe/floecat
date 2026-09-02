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

package ai.floedb.floecat.service.storage;

import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.service.storage.StoreReadObserver.Operation;
import ai.floedb.floecat.service.storage.StoreReadObserver.ReadCall;
import ai.floedb.floecat.service.storage.StoreReadObserver.Store;
import ai.floedb.floecat.storage.spi.BlobStore;
import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/** Adds read observability to whichever blob-store adapter the deployment selects. */
@Decorator
@Priority(Interceptor.Priority.APPLICATION)
public final class ObservedBlobStore implements BlobStore {
  private final BlobStore delegate;
  private final StoreReadObserver observer;

  @Inject
  public ObservedBlobStore(@Delegate @Any BlobStore delegate, StoreReadObserver observer) {
    this.delegate = delegate;
    this.observer = observer;
  }

  @Override
  public byte[] get(String uri) {
    return observe(
        Operation.GET,
        1,
        Collections.singletonList(uri),
        () -> delegate.get(uri),
        ObservedBlobStore::bytes);
  }

  @Override
  public byte[] getRange(String uri, long offset, int length) {
    return observe(
        Operation.GET_RANGE,
        1,
        Collections.singletonList(uri),
        () -> delegate.getRange(uri, offset, length),
        ObservedBlobStore::bytes);
  }

  @Override
  public void put(String uri, byte[] bytes, String contentType) {
    delegate.put(uri, bytes, contentType);
  }

  @Override
  public void putImmutable(String uri, byte[] bytes, String contentType) {
    delegate.putImmutable(uri, bytes, contentType);
  }

  @Override
  public Optional<BlobHeader> head(String uri) {
    return observe(Operation.HEAD, 1, Collections.singletonList(uri), () -> delegate.head(uri));
  }

  @Override
  public boolean delete(String uri) {
    return delegate.delete(uri);
  }

  @Override
  public boolean supportsVersionedDeletes() {
    return delegate.supportsVersionedDeletes();
  }

  @Override
  public boolean delete(String uri, String versionId) {
    return delegate.delete(uri, versionId);
  }

  @Override
  public int deletePrefix(String prefix) {
    return delegate.deletePrefix(prefix);
  }

  @Override
  public Map<String, byte[]> getBatch(List<String> uris) {
    if (uris == null || uris.isEmpty()) {
      return delegate.getBatch(uris);
    }
    return observe(
        Operation.GET_BATCH,
        uris.size(),
        uris,
        () -> delegate.getBatch(uris),
        ObservedBlobStore::totalBytes);
  }

  @Override
  public Map<Range, byte[]> getRanges(List<Range> ranges) {
    if (ranges == null || ranges.isEmpty()) {
      return delegate.getRanges(ranges);
    }
    return observe(
        Operation.GET_RANGES,
        ranges.size(),
        ranges.stream().map(Range::uri).toList(),
        () -> delegate.getRanges(ranges),
        ObservedBlobStore::totalBytes);
  }

  @Override
  public Page list(String prefix, int limit, String pageToken) {
    return observe(
        Operation.LIST,
        0,
        Collections.singletonList(prefix),
        () -> delegate.list(prefix, limit, pageToken));
  }

  @Override
  public Page listPrefixes(String prefix, int limit, String pageToken) {
    return observe(
        Operation.LIST_PREFIXES,
        0,
        Collections.singletonList(prefix),
        () -> delegate.listPrefixes(prefix, limit, pageToken));
  }

  private <T> T observe(Operation operation, int items, List<String> targets, Supplier<T> body) {
    return StoreReadInstrumentation.observe(
        observer, new ReadCall(Store.BLOB, operation, items, observedTargets(targets)), body);
  }

  private <T> T observe(
      Operation operation,
      int items,
      List<String> targets,
      Supplier<T> body,
      ToLongFunction<T> bytes) {
    return StoreReadInstrumentation.observe(
        observer,
        new ReadCall(Store.BLOB, operation, items, observedTargets(targets)),
        body,
        bytes);
  }

  private List<String> observedTargets(List<String> targets) {
    return observer.capturesTargets() ? targets : List.of();
  }

  private static long bytes(byte[] value) {
    return value == null ? 0L : value.length;
  }

  private static long totalBytes(Map<?, byte[]> values) {
    if (values == null) {
      return 0L;
    }
    return values.values().stream()
        .filter(java.util.Objects::nonNull)
        .mapToLong(ObservedBlobStore::bytes)
        .sum();
  }
}

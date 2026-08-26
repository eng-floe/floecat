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
package ai.floedb.floecat.storage.kv.dynamodb.ps;

import ai.floedb.floecat.aws.ClosedAwsClientDetector;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.telemetry.StorageTelemetry;
import ai.floedb.floecat.telemetry.StorageTelemetry.Backend;
import ai.floedb.floecat.telemetry.StorageTelemetry.Call;
import ai.floedb.floecat.telemetry.StorageTelemetry.Measurement;
import ai.floedb.floecat.telemetry.StorageTelemetry.Operation;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Synchronous adapter that preserves the existing {@link PointerStore} contract while delegating
 * storage to the Mutiny-based {@link PointerStoreEntity}.
 */
public abstract class KvPointerStore implements PointerStore {

  private final PointerStoreEntity pointers;
  private final StorageTelemetry telemetry;

  public KvPointerStore(PointerStoreEntity pointers, StorageTelemetry telemetry) {
    this.pointers = pointers;
    this.telemetry = telemetry;
  }

  @Override
  public Optional<Pointer> get(String key) {
    return observeDynamo(
        Operation.GET,
        () -> pointers.get(key).await().indefinitely(),
        value ->
            value
                .map(pointer -> Measurement.of(pointer.getSerializedSize(), 1L))
                .orElseGet(Measurement::notFound));
  }

  @Override
  public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
    return observeDynamo(
        Operation.COMPARE_AND_SET,
        () -> pointers.compareAndSet(key, expectedVersion, next).await().indefinitely(),
        ignored -> Measurement.of(next.getSerializedSize(), 1L));
  }

  @Override
  public boolean delete(String key) {
    return observeDynamo(
        Operation.DELETE,
        () -> pointers.delete(key).await().indefinitely(),
        ignored -> Measurement.of(0L, 1L));
  }

  @Override
  public boolean compareAndDelete(String key, long expectedVersion) {
    return observeDynamo(
        Operation.COMPARE_AND_DELETE,
        () -> pointers.compareAndDelete(key, expectedVersion).await().indefinitely(),
        ignored -> Measurement.of(0L, 1L));
  }

  @Override
  public boolean compareAndSetBatch(List<CasOp> ops) {
    return observeDynamo(
        Operation.COMPARE_AND_SET_BATCH,
        () -> pointers.compareAndSetBatch(ops).await().indefinitely(),
        ignored ->
            Measurement.of(
                ops == null
                    ? 0L
                    : ops.stream()
                        .mapToLong(
                            op ->
                                op instanceof CasUpsert upsert
                                    ? upsert.next().getSerializedSize()
                                    : 0L)
                        .sum(),
                ops == null ? 0L : ops.size()));
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    Optional<String> token =
        (pageToken == null || pageToken.isBlank()) ? Optional.empty() : Optional.of(pageToken);

    var page =
        observeDynamo(
            Operation.LIST_BY_PREFIX,
            () -> pointers.listByPrefix(prefix, limit, token).await().indefinitely(),
            value ->
                Measurement.of(
                    value.items().stream().mapToLong(Pointer::getSerializedSize).sum(),
                    value.items().size()));

    if (nextTokenOut != null) {
      nextTokenOut.setLength(0);
      page.nextToken().ifPresent(nextTokenOut::append);
    }

    return List.copyOf(page.items());
  }

  @Override
  public String pageTokenAfterKey(String key) {
    return pointers.pageTokenAfterKey(key);
  }

  @Override
  public int deleteByPrefix(String prefix) {
    return observeDynamo(
        Operation.DELETE_BY_PREFIX,
        () -> pointers.deleteByPrefix(prefix).await().indefinitely(),
        count -> Measurement.of(0L, count));
  }

  @Override
  public int countByPrefix(String prefix) {
    return observeDynamo(
        Operation.COUNT_BY_PREFIX,
        () -> countByPrefixInternal(prefix),
        count -> Measurement.of(0L, count));
  }

  /** Counts all key pages while keeping the public operation as one logical observation. */
  private int countByPrefixInternal(String prefix) {
    int count = 0;

    Optional<String> token = Optional.empty();
    do {
      Optional<String> pageToken = token;
      var page =
          executeDynamo(
              () -> pointers.listKeysByPrefix(prefix, 500, pageToken).await().indefinitely());

      count += page.items().size();
      token = page.nextToken();
    } while (token.isPresent());

    return count;
  }

  @Override
  public boolean isEmpty() {
    return observeDynamo(Operation.IS_EMPTY, pointers::isEmpty, ignored -> Measurement.none());
  }

  @Override
  public void dump(String header) {
    observeDynamo(
        Operation.DUMP,
        () -> {
          pointers.getKvStore().dump(header).await().indefinitely();
          return null;
        },
        ignored -> Measurement.none());
  }

  /**
   * Applies the shared timing, result, metric, and request-summary contract to one DynamoDB pointer
   * operation.
   */
  private <T> T observeDynamo(
      Operation operation, Supplier<T> work, java.util.function.Function<T, Measurement> measure) {
    return telemetry.observe(
        new Call(Backend.DYNAMODB, operation), () -> executeDynamo(work), measure);
  }

  /** Preserves store failures while normalizing closed AWS client generations to a retry signal. */
  private static <T> T executeDynamo(Supplier<T> work) {
    try {
      return work.get();
    } catch (StorageAbortRetryableException e) {
      throw e;
    } catch (RuntimeException e) {
      if (!ClosedAwsClientDetector.isConnectionPoolShutdown(e)) {
        throw e;
      }
      throw new StorageAbortRetryableException("DynamoDB pointer store operation failed", e);
    }
  }
}

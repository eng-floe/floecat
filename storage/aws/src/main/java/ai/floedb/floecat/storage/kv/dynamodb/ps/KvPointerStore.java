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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.PointerStore;
import java.util.List;
import java.util.Optional;

/**
 * Synchronous adapter that preserves the existing {@link PointerStore} contract while delegating
 * storage to the Mutiny-based {@link PointerStoreEntity}.
 */
public abstract class KvPointerStore implements PointerStore {

  private final PointerStoreEntity pointers;

  public KvPointerStore(PointerStoreEntity pointers) {
    this.pointers = pointers;
  }

  @Override
  public Optional<Pointer> get(String key) {
    return pointers.get(key).await().indefinitely();
  }

  @Override
  public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
    return pointers.compareAndSet(key, expectedVersion, next).await().indefinitely();
  }

  @Override
  public boolean delete(String key) {
    return pointers.delete(key).await().indefinitely();
  }

  @Override
  public boolean compareAndDelete(String key, long expectedVersion) {
    return pointers.compareAndDelete(key, expectedVersion).await().indefinitely();
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {

    Optional<String> token =
        (pageToken == null || pageToken.isBlank()) ? Optional.empty() : Optional.of(pageToken);

    var page = pointers.listByPrefix(prefix, limit, token).await().indefinitely();

    nextTokenOut.setLength(0);
    page.nextToken().ifPresent(nextTokenOut::append);

    return List.copyOf(page.items());
  }

  @Override
  public int deleteByPrefix(String prefix) {
    return pointers.deleteByPrefix(prefix).await().indefinitely();
  }

  @Override
  public int countByPrefix(String prefix) {
    int count = 0;

    Optional<String> token = Optional.empty();
    do {
      var page = pointers.listKeysByPrefix(prefix, 500, token).await().indefinitely();

      count += page.items().size();
      token = page.nextToken();
    } while (token.isPresent());

    return count;
  }

  @Override
  public boolean isEmpty() {
    return pointers.isEmpty();
  }

  @Override
  public void dump(String header) {
    pointers.getKvStore().dump(header).await().indefinitely();
  }
}

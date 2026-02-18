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
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Synchronous adapter that preserves the existing {@link PointerStore} contract while delegating
 * storage to the Mutiny-based {@link PointerStoreEntity}.
 */
public abstract class KvPointerStore implements PointerStore {

  private static final int BACKEND_PAGE_LIMIT = 1000;

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
  public boolean compareAndSetBatch(List<CasOp> ops) {
    return pointers.compareAndSetBatch(ops).await().indefinitely();
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    final int requestedLimit = Math.max(1, limit);
    final String startAfter = (pageToken == null || pageToken.isBlank()) ? null : pageToken;
    final int backendLimit = Math.min(BACKEND_PAGE_LIMIT, requestedLimit);

    Optional<String> backendToken = Optional.empty();
    List<Pointer> out = new ArrayList<>(Math.min(requestedLimit, BACKEND_PAGE_LIMIT));
    boolean hasMore = false;
    boolean tokenSeen = (startAfter == null);

    while (true) {
      var page = pointers.listByPrefix(prefix, backendLimit, backendToken).await().indefinitely();
      for (Pointer pointer : page.items()) {
        if (!tokenSeen) {
          int cmp = pointer.getKey().compareTo(startAfter);
          if (cmp < 0) {
            continue;
          }
          if (cmp == 0) {
            tokenSeen = true;
            continue;
          }
          throw new IllegalArgumentException("bad page token");
        }
        if (startAfter != null && pointer.getKey().compareTo(startAfter) <= 0) {
          continue;
        }
        if (out.size() < requestedLimit) {
          out.add(pointer);
        } else {
          hasMore = true;
          break;
        }
      }
      if (hasMore || page.nextToken().isEmpty()) {
        break;
      }
      backendToken = page.nextToken();
    }

    if (!tokenSeen) {
      throw new IllegalArgumentException("bad page token");
    }

    if (nextTokenOut != null) {
      nextTokenOut.setLength(0);
      if (hasMore && !out.isEmpty()) {
        nextTokenOut.append(out.get(out.size() - 1).getKey());
      }
    }
    return List.copyOf(out);
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

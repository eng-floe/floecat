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

package ai.floedb.floecat.storage.spi;

import ai.floedb.floecat.common.rpc.BlobHeader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface BlobStore {
  byte[] get(String uri);

  void put(String uri, byte[] bytes, String contentType);

  Optional<BlobHeader> head(String uri);

  boolean delete(String uri);

  /**
   * Deletes only the blob version that {@code versionId} names (as observed via {@link
   * BlobHeader#getVersionId()}). A different version — in particular one written concurrently after
   * the caller's {@link #head} — must survive, so a check-then-delete caller acts on exactly the
   * object it checked. Returns true when the named version was deleted (or already absent); false
   * when the store can tell the blob has moved past it and nothing was deleted.
   *
   * <p>A blank {@code versionId} (store without versioning) and the default implementation both
   * fall back to the unconditional {@link #delete(String)}; version-capable stores must override.
   */
  default boolean delete(String uri, String versionId) {
    return delete(uri);
  }

  void deletePrefix(String prefix);

  default Map<String, byte[]> getBatch(List<String> uris) {
    Map<String, byte[]> out = new HashMap<>(uris.size());
    for (String u : uris) out.put(u, get(u));
    return out;
  }

  interface Page {
    List<String> keys();

    String nextToken();
  }

  Page list(String prefix, int limit, String pageToken);
}

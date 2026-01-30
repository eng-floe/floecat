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

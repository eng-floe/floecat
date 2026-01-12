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

package ai.floedb.floecat.storage;

import ai.floedb.floecat.common.rpc.Pointer;
import java.util.List;
import java.util.Optional;

public interface PointerStore {
  Optional<Pointer> get(String key);

  boolean compareAndSet(String key, long expectedVersion, Pointer next);

  boolean delete(String key);

  boolean compareAndDelete(String key, long expectedVersion);

  List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut);

  int deleteByPrefix(String prefix);

  int countByPrefix(String prefix);

  boolean isEmpty();

  default void dump(String header) {}
}

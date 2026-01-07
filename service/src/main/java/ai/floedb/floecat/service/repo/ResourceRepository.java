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

package ai.floedb.floecat.service.repo;

import java.util.List;
import java.util.Optional;

public interface ResourceRepository<T> {
  Optional<T> get(String key);

  void putBlob(String blobUri, T value);

  void advancePointer(String key, String blobUri, long expectedVersion);

  List<T> listByPrefix(String prefix, int limit, String pageToken, StringBuilder nextTokenOut);

  int countByPrefix(String prefix);
}

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
package ai.floedb.floecat.storage.kv;

import java.util.Set;

public interface KvAttributes {
  String ATTR_PARTITION_KEY = "pk";
  String ATTR_SORT_KEY = "sk";
  String ATTR_KIND = "kind";
  String ATTR_VALUE = "value";
  String ATTR_VERSION = "version";
  String ATTR_TTL = "ttl";
  String ATTR_EXPIRES_AT = "timestamp";

  String TARGET_PARTITION_KEY = "targetPk";
  String TARGET_SORT_KEY = "targetSk";

  /**
   * Names a backend uses for the record's own structure. They are reserved: a record attribute may
   * not use one, because storing it would collide with the structural field of the same name.
   */
  Set<String> STRUCTURAL_ATTRS =
      Set.of(ATTR_PARTITION_KEY, ATTR_SORT_KEY, ATTR_KIND, ATTR_VALUE, ATTR_VERSION);
}

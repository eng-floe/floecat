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
   * Names the backend gives its own meaning; a record attribute may not use one. Most would collide
   * with the structural field of the same name. {@link #ATTR_TTL} is quieter: DynamoDB's TTL
   * feature is enabled on that attribute, so a numeric attr named {@code ttl} silently schedules
   * the row's deletion.
   *
   * <p>Reserved on writes only — rows written by others may carry these names, so decoders drop
   * them rather than refusing the record.
   */
  Set<String> RESERVED_ATTRS =
      Set.of(ATTR_PARTITION_KEY, ATTR_SORT_KEY, ATTR_KIND, ATTR_VALUE, ATTR_VERSION, ATTR_TTL);
}

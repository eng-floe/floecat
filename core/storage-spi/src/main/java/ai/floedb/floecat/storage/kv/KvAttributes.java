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
   * Names a backend gives its own meaning, on a record it also stores attributes on. They are
   * reserved: a record attribute may not use one. For {@link #ATTR_PARTITION_KEY}, {@link
   * #ATTR_SORT_KEY}, {@link #ATTR_KIND}, {@link #ATTR_VALUE} and {@link #ATTR_VERSION} the harm is
   * a collision with the structural field of the same name. For {@link #ATTR_TTL} it is worse and
   * quieter: {@code DynamoDbTablesBootstrap} enables DynamoDB's TTL feature on that attribute, so
   * an ordinary numeric attr that happens to be named {@code ttl} is read by DynamoDB as the row's
   * expiry time and the row is deleted once it passes — no error, no trace.
   *
   * <p>Reserved on the way in only. A row that some other writer already put one of these names on
   * still has to be readable, so decoders drop them rather than refusing the record (see {@code
   * DynamoDbKvStore#avToAttrs}).
   */
  Set<String> RESERVED_ATTRS =
      Set.of(ATTR_PARTITION_KEY, ATTR_SORT_KEY, ATTR_KIND, ATTR_VALUE, ATTR_VERSION, ATTR_TTL);
}

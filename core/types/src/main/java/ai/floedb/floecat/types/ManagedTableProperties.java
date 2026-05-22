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

package ai.floedb.floecat.types;

import java.util.Set;

/** Shared table-property keys managed by the Iceberg-compatible metadata layer. */
public final class ManagedTableProperties {

  public static final String TABLE_UUID = "table-uuid";
  public static final String FORMAT_VERSION = "format-version";
  public static final String CURRENT_SCHEMA_ID = "current-schema-id";
  public static final String LAST_COLUMN_ID = "last-column-id";
  public static final String DEFAULT_SPEC_ID = "default-spec-id";
  public static final String LAST_PARTITION_ID = "last-partition-id";
  public static final String DEFAULT_SORT_ORDER_ID = "default-sort-order-id";
  public static final String CURRENT_SNAPSHOT_ID = "current-snapshot-id";
  public static final String LAST_SEQUENCE_NUMBER = "last-sequence-number";
  public static final String METADATA_REFS = "metadata.refs";

  private static final Set<String> ENGINE_MANAGED_KEYS =
      Set.of(
          TABLE_UUID,
          FORMAT_VERSION,
          CURRENT_SCHEMA_ID,
          LAST_COLUMN_ID,
          DEFAULT_SPEC_ID,
          LAST_PARTITION_ID,
          DEFAULT_SORT_ORDER_ID,
          CURRENT_SNAPSHOT_ID,
          LAST_SEQUENCE_NUMBER,
          METADATA_REFS);

  private static final Set<String> TABLE_DEFINITION_KEYS =
      Set.of(
          FORMAT_VERSION,
          LAST_COLUMN_ID,
          CURRENT_SCHEMA_ID,
          DEFAULT_SPEC_ID,
          LAST_PARTITION_ID,
          DEFAULT_SORT_ORDER_ID);

  private ManagedTableProperties() {}

  /** Keys owned by metadata commit logic and not by reconcile descriptor payloads. */
  public static Set<String> engineManagedKeys() {
    return ENGINE_MANAGED_KEYS;
  }

  /** Keys that can be mutated by table-definition commit updates. */
  public static Set<String> tableDefinitionKeys() {
    return TABLE_DEFINITION_KEYS;
  }
}

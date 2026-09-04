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

package ai.floedb.floecat.client.unity;

import java.util.List;
import java.util.Map;

/** Normalized table metadata, independent of Unity Catalog's JSON representation. */
public record UnityCatalogTable(
    String name,
    String tableId,
    String tableType,
    String dataSourceFormat,
    String storageLocation,
    String viewDefinition,
    List<Column> columns,
    Map<String, String> properties) {

  /**
   * Normalizes the two components callers treat as always-present.
   *
   * <p>{@code name} is never null even when the catalog omits it or sends a blank string: it is the
   * sort key for {@code listTables} and {@code listViewDescriptors}, and {@code Stream.sorted()}
   * uses natural ordering, so a single nameless entry in a page would fail the whole listing with a
   * NullPointerException rather than degrade. Every other component stays nullable, because absent
   * is meaningful for them -- a null {@code storageLocation} is "not an external table", not "".
   */
  public UnityCatalogTable {
    name = name == null ? "" : name;
    columns = columns == null ? List.of() : List.copyOf(columns);
    properties = properties == null ? Map.of() : Map.copyOf(properties);
  }

  /**
   * One column of a Unity table.
   *
   * <p>{@code partitionIndex} is the column's ordinal in the partition spec, and {@code null} for a
   * column that does not partition. Dropping it recorded every partitioned Delta table reached
   * through a Unity Overlay as unpartitioned, which loses partition pruning for every query against
   * it.
   */
  public record Column(
      String name,
      String typeName,
      String typeText,
      String typeJson,
      boolean nullable,
      Integer partitionIndex) {
    public Column {
      name = name == null ? "" : name;
    }

    /** A column that does not partition, which is most of them. */
    public Column(
        String name, String typeName, String typeText, String typeJson, boolean nullable) {
      this(name, typeName, typeText, typeJson, nullable, null);
    }
  }
}

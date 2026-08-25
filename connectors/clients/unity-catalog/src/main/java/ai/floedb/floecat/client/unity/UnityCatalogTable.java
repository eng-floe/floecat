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

  public UnityCatalogTable {
    columns = columns == null ? List.of() : List.copyOf(columns);
    properties = properties == null ? Map.of() : Map.copyOf(properties);
  }

  public record Column(
      String name, String typeName, String typeText, String typeJson, boolean nullable) {}
}

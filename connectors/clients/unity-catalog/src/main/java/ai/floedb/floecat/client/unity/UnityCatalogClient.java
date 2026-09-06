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
import java.util.Optional;

/** A small, transport-independent boundary for the Unity Catalog operations Floecat uses. */
public interface UnityCatalogClient extends AutoCloseable {
  List<String> listCatalogs();

  List<String> listSchemas(String catalogName);

  List<UnityCatalogTable> listTables(String catalogName, String schemaName);

  /**
   * One table, or empty when the catalog does not have it.
   *
   * <p>Column metadata is parsed strictly here, unlike {@link #listTables}: a {@code columns} field
   * that is present but not an array of column objects fails the call rather than yielding a table
   * with no columns, because a silently empty schema reported as authoritative is worse than a
   * failure.
   *
   * <p>Callers that do not read the schema must use {@link #getTableWithLenientColumns} instead:
   * that strictness otherwise reaches past schema reporting into planning, which never looks at the
   * column list.
   */
  Optional<UnityCatalogTable> getTable(String fullName);

  /**
   * The same table, with a malformed {@code columns} field yielding no columns rather than failing
   * the call.
   *
   * <p>For the callers that want the storage location. Strict decoding is right where the schema is
   * the answer -- a silently empty schema reported as authoritative is worse than a failure -- and
   * wrong where it is not: one table whose catalog renders {@code columns} in an unexpected shape
   * would otherwise fail snapshot planning and file-group capture, neither of which reads a column,
   * and a describe with a storage location prefers the Delta log's schema over this one anyway.
   * Degrading schema reporting for that table is the whole of the cost.
   */
  Optional<UnityCatalogTable> getTableWithLenientColumns(String fullName);

  TemporaryTableCredentials generateTemporaryTableCredentials(
      String tableId, TableOperation operation);

  /**
   * Releases the transport this client owns.
   *
   * <p>Declared here, and narrowed to throw nothing, because the connector that owns a client is
   * built per vend -- once per scan session and once per file group -- so an implementation holding
   * a pooled transport leaks a thread and an executor on every call unless the owner can close it.
   */
  @Override
  void close();

  enum TableOperation {
    READ,
    READ_WRITE
  }
}

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

package ai.floedb.floecat.catalog.access;

import java.util.List;
import java.util.Optional;

/** Discovery and metadata access to one upstream catalog connection. */
public interface CatalogClient extends AutoCloseable {
  CatalogCapabilities capabilities();

  void validate();

  List<NamespacePath> listNamespaces(NamespacePath parent);

  List<CatalogObjectName> listTables(NamespacePath namespace);

  CatalogTable loadTable(CatalogObjectName table);

  List<CatalogObjectName> listViews(NamespacePath namespace);

  CatalogView loadView(CatalogObjectName view);

  /**
   * Re-loads the table through the upstream protocol and returns only credentials from its
   * dedicated vending channel. Call again when credentials expire; an absent expiry must not be
   * treated as non-expiring.
   */
  Optional<VendedStorageCredentials> vendStorageCredentials(CatalogObjectName table);

  /**
   * Performs a non-mutating read of upstream table storage using credentials vended through the
   * provider protocol. A successful return proves that the storage credential boundary is usable.
   */
  void validateStorageAccess(CatalogObjectName table);

  @Override
  void close();
}

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

package ai.floedb.floecat.connector.iceberg.impl;

import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;

final class IcebergFilesystemConnector extends IcebergConnector {

  IcebergFilesystemConnector(
      String connectorId,
      Table table,
      String namespaceFq,
      String tableName,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles,
      FileIO externalFileIO) {
    super(
        connectorId,
        table,
        namespaceFq,
        tableName,
        ndvEnabled,
        ndvSampleFraction,
        ndvMaxFiles,
        externalFileIO);
  }

  @Override
  public List<String> listNamespaces() {
    return listNamespacesSingle();
  }

  @Override
  public List<String> listTables(String namespaceFq) {
    return listTablesSingle(namespaceFq);
  }

  @Override
  protected Table loadTableFromSource(String namespaceFq, String tableName) {
    return getSingleTable();
  }
}

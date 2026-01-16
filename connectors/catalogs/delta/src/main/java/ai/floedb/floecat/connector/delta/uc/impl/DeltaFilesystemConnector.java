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

package ai.floedb.floecat.connector.delta.uc.impl;

import io.delta.kernel.engine.Engine;
import java.util.List;
import java.util.function.Function;
import org.apache.parquet.io.InputFile;

final class DeltaFilesystemConnector extends DeltaConnector {

  private final String namespaceFq;
  private final String tableName;
  private final String tableRoot;

  DeltaFilesystemConnector(
      String connectorId,
      Engine engine,
      Function<String, InputFile> parquetInput,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles,
      String tableRoot,
      String namespaceFq,
      String tableName) {
    super(connectorId, engine, parquetInput, ndvEnabled, ndvSampleFraction, ndvMaxFiles);
    this.tableRoot = tableRoot;
    this.namespaceFq = namespaceFq == null ? "" : namespaceFq;
    this.tableName = tableName == null ? "" : tableName;
  }

  @Override
  public List<String> listNamespaces() {
    return namespaceFq.isBlank() ? List.of() : List.of(namespaceFq);
  }

  @Override
  public List<String> listTables(String namespaceFq) {
    if (tableName.isBlank()) {
      return List.of();
    }
    if (this.namespaceFq.isBlank()) {
      return List.of(tableName);
    }
    return this.namespaceFq.equals(namespaceFq) ? List.of(tableName) : List.of();
  }

  @Override
  public TableDescriptor describe(String namespaceFq, String tableName) {
    String effectiveNs = this.namespaceFq.isBlank() ? namespaceFq : this.namespaceFq;
    String effectiveTable = this.tableName.isBlank() ? tableName : this.tableName;
    return describeFromDelta(tableRoot, effectiveNs, effectiveTable);
  }

  @Override
  protected String storageLocation(String namespaceFq, String tableName) {
    return tableRoot;
  }
}

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

final class DeltaGlueConnector extends DeltaConnector {

  private final GlueDeltaCatalog glueCatalog;

  DeltaGlueConnector(
      String connectorId,
      GlueDeltaCatalog glueCatalog,
      Engine engine,
      Function<String, InputFile> parquetInput,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles) {
    super(connectorId, engine, parquetInput, ndvEnabled, ndvSampleFraction, ndvMaxFiles);
    this.glueCatalog = glueCatalog;
  }

  @Override
  public List<String> listNamespaces() {
    return glueCatalog.deltaNamespaces();
  }

  @Override
  public List<String> listTables(String namespaceFq) {
    return glueCatalog.deltaTables(namespaceFq);
  }

  @Override
  public TableDescriptor describe(String namespaceFq, String tableName) {
    String location = glueCatalog.storageLocation(namespaceFq, tableName);
    return describeFromDelta(location, namespaceFq, tableName);
  }

  @Override
  protected String storageLocation(String namespaceFq, String tableName) {
    return glueCatalog.storageLocation(namespaceFq, tableName);
  }

  @Override
  public void close() {
    glueCatalog.close();
  }
}

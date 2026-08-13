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

package ai.floedb.floecat.service.storage.impl;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import java.util.List;

final class StorageAuthorityLookupHints {
  private static final String S3TABLES_WAREHOUSE_ARN_PREFIX = "arn:aws:s3tables:";

  private StorageAuthorityLookupHints() {}

  static List<String> forTable(Table table, ConnectorRepository connectorRepo) {
    if (table == null
        || connectorRepo == null
        || !table.hasUpstream()
        || !table.getUpstream().hasConnectorId()) {
      return List.of();
    }
    Connector connector = connectorRepo.getById(table.getUpstream().getConnectorId()).orElse(null);
    return forConnector(connector);
  }

  static List<String> forConnector(Connector connector) {
    if (connector == null) {
      return List.of();
    }
    return forConfig(ConnectorConfigMapper.fromProto(connector));
  }

  static List<String> forConfig(ConnectorConfig config) {
    if (config == null || config.options() == null) {
      return List.of();
    }
    String warehouse = config.options().get("warehouse");
    if (warehouse == null || warehouse.isBlank()) {
      return List.of();
    }
    if (config.kind() != ConnectorConfig.Kind.ICEBERG
        || !warehouse.regionMatches(true, 0, S3TABLES_WAREHOUSE_ARN_PREFIX, 0,
            S3TABLES_WAREHOUSE_ARN_PREFIX.length())) {
      return List.of();
    }
    return List.of(warehouse.trim());
  }
}

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

package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import java.util.Objects;
import org.jboss.logging.Logger;

final class ConnectorTypeResolver {

  private static final Logger LOG = Logger.getLogger(ConnectorTypeResolver.class);

  private ConnectorTypeResolver() {}

  static String connectorTypeFor(TableRepository tableRepository, ResourceId tableId) {
    Objects.requireNonNull(tableRepository, "tableRepository");
    Objects.requireNonNull(tableId, "tableId");
    return tableRepository
        .getById(tableId)
        .filter(Table::hasUpstream)
        .map(table -> table.getUpstream().getFormat())
        .map(
            format ->
                switch (format) {
                  case TF_ICEBERG -> "iceberg";
                  case TF_DELTA -> "delta";
                  default -> {
                    LOG.debugf(
                        "No stats connector type mapping for table=%s format=%s", tableId, format);
                    yield "";
                  }
                })
        .orElse("");
  }
}

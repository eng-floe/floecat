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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.gateway.iceberg.rest.api.request.MetricsRequests;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.TableRequestContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableMetricsService {
  private static final Logger LOG = Logger.getLogger(TableMetricsService.class);

  @Inject ObjectMapper mapper;

  public Response publish(TableRequestContext tableContext, MetricsRequests.Report request) {
    if (request == null) {
      return IcebergErrorResponses.validation("Request body is required");
    }
    if (isBlank(request.reportType())) {
      return IcebergErrorResponses.validation("report-type is required");
    }
    if (isBlank(request.tableName())) {
      return IcebergErrorResponses.validation("table-name is required");
    }
    if (request.snapshotId() == null) {
      return IcebergErrorResponses.validation("snapshot-id is required");
    }
    if (request.metrics() == null) {
      return IcebergErrorResponses.validation("metrics is required");
    }
    boolean scanReport =
        request.filter() != null
            && request.schemaId() != null
            && !isEmpty(request.projectedFieldIds())
            && !isEmpty(request.projectedFieldNames());
    boolean commitReport = request.sequenceNumber() != null && !isBlank(request.operation());
    if (!scanReport && !commitReport) {
      return IcebergErrorResponses.validation("metrics report missing required fields");
    }
    try {
      LOG.infof(
          "Received metrics report namespace=%s table=%s payload=%s",
          tableContext.namespaceName(), tableContext.table(), mapper.writeValueAsString(request));
    } catch (JsonProcessingException e) {
      LOG.infof(
          "Received metrics report namespace=%s table=%s payload=%s",
          tableContext.namespaceName(), tableContext.table(), String.valueOf(request));
    }
    return Response.noContent().build();
  }

  private boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private boolean isEmpty(List<?> values) {
    return values == null || values.isEmpty();
  }
}

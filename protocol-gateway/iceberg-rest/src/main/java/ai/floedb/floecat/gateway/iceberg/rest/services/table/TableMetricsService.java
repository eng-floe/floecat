package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.gateway.iceberg.rest.api.request.MetricsRequests;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.TableRequestContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableMetricsService {
  private static final Logger LOG = Logger.getLogger(TableMetricsService.class);

  @Inject ObjectMapper mapper;

  public Response publish(TableRequestContext tableContext, MetricsRequests.Report request) {
    if (request == null || request.snapshotId() == null) {
      return IcebergErrorResponses.validation("snapshot-id is required");
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
}

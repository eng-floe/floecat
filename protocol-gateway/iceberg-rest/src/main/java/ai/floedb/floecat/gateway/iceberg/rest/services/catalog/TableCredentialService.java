package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CredentialsResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.TableRequestContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
public class TableCredentialService {

  public Response load(TableRequestContext tableContext, String planId, TableGatewaySupport tableSupport) {
    // hook for future credential resolution per plan/table context
    return Response.ok(new CredentialsResponseDto(tableSupport.defaultCredentials())).build();
  }
}

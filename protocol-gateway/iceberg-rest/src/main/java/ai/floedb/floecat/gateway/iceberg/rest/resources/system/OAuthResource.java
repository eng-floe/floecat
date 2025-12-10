package ai.floedb.floecat.gateway.iceberg.rest.resources.system;

import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

@Path("/v1/oauth/tokens")
@Produces(MediaType.APPLICATION_JSON)
public class OAuthResource {
  private static final Logger LOG = Logger.getLogger(OAuthResource.class);

  @POST
  public Response unsupportedTokenExchange() {
    LOG.warn("Received oauth/tokens request, but the endpoint is not supported");
    return Response.status(Response.Status.NOT_IMPLEMENTED)
        .entity(
            """
            {
              "error": {
                "type": "UnsupportedOperationException",
                "message": "POST /v1/oauth/tokens is not supported by this catalog implementation"
              }
            }
            """)
        .build();
  }
}

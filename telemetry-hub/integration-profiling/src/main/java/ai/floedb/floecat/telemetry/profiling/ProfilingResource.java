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

package ai.floedb.floecat.telemetry.profiling;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.nio.file.Files;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/profiling")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ProfilingResource {
  private static final Logger LOG = LoggerFactory.getLogger(ProfilingResource.class);
  @Inject ProfilingCaptureService captureService;
  @Inject ProfilingConfig config;

  @POST
  @Path("/captures")
  public Response startCapture(ProfilingRequest request) {
    if (!config.endpointsEnabled()) {
      Map<String, String> error =
          Map.of("error", "endpoints_disabled", "message", "profiling endpoints disabled");
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(error).build();
    }
    try {
      CaptureMetadata meta =
          captureService.startCapture(
              request.trigger,
              request.duration,
              request.mode,
              request.scope,
              request.requestedBy,
              null);
      return Response.accepted(meta).build();
    } catch (ProfilingException e) {
      int statusCode;
      switch (e.reason()) {
        case DISABLED:
          statusCode = Response.Status.SERVICE_UNAVAILABLE.getStatusCode();
          break;
        case RATE_LIMIT:
          statusCode = Response.Status.TOO_MANY_REQUESTS.getStatusCode();
          break;
        case ALREADY_RUNNING:
          statusCode = Response.Status.CONFLICT.getStatusCode();
          break;
        case DISK_CAP:
          statusCode = 507;
          break;
        case UNSUPPORTED_MODE:
          statusCode = Response.Status.BAD_REQUEST.getStatusCode();
          break;
        default:
          statusCode = Response.Status.BAD_REQUEST.getStatusCode();
      }
      Map<String, String> error = Map.of("error", e.reason().tagValue(), "message", e.getMessage());
      return Response.status(statusCode).entity(error).build();
    } catch (RuntimeException e) {
      LOG.error("profiling capture failed", e);
      Map<String, String> error =
          Map.of("error", "internal_error", "message", "internal server error");
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(error).build();
    }
  }

  @GET
  @Path("/captures/latest")
  public CaptureMetadata latestCapture() {
    return captureService
        .latest()
        .orElseThrow(() -> new NotFoundException("no profiling captures yet"));
  }

  @GET
  @Path("/captures/{id}")
  public CaptureMetadata captureById(@PathParam("id") String id) {
    return captureService
        .find(id)
        .orElseThrow(() -> new NotFoundException("capture not found: " + id));
  }

  @GET
  @Path("/captures/{id}/artifact")
  @Produces("application/x-java-flight-recording")
  public Response artifactById(@PathParam("id") String id) {
    CaptureMetadata meta =
        captureService
            .find(id)
            .orElseThrow(() -> new NotFoundException("capture not found: " + id));
    java.nio.file.Path artifact = captureService.artifactPath(id);
    if (Files.notExists(artifact)) {
      throw new NotFoundException("artifact not found: " + id);
    }
    StreamingOutput stream = os -> Files.copy(artifact, os);
    return Response.ok(stream)
        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=profiling-" + id + ".jfr")
        .build();
  }
}

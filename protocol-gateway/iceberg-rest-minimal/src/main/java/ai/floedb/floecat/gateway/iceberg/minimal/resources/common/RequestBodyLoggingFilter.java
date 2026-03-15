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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.common;

import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.ext.Provider;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@Provider
@Priority(Priorities.USER)
public class RequestBodyLoggingFilter implements ContainerRequestFilter {
  private static final Logger LOG = Logger.getLogger(RequestBodyLoggingFilter.class);

  @Inject
  @ConfigProperty(name = "floecat.gateway.minimal.log-request-bodies", defaultValue = "false")
  boolean logRequestBodies;

  @Inject
  @ConfigProperty(
      name = "floecat.gateway.minimal.log-request-body-max-chars",
      defaultValue = "8192")
  int logRequestBodyMaxChars;

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    if (!logRequestBodies) {
      return;
    }
    if (!hasEntityMethod(requestContext.getMethod())) {
      return;
    }
    MediaType mediaType = requestContext.getMediaType();
    if (mediaType == null || !mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE)) {
      return;
    }
    InputStream entityStream = requestContext.getEntityStream();
    byte[] body = entityStream.readAllBytes();
    requestContext.setEntityStream(new ByteArrayInputStream(body));

    String payload = new String(body, StandardCharsets.UTF_8);
    int maxChars = Math.max(256, logRequestBodyMaxChars);
    boolean truncated = payload.length() > maxChars;
    if (truncated) {
      payload = payload.substring(0, maxChars);
    }

    LOG.infof(
        "HTTP %s %s payload=%s%s",
        requestContext.getMethod(),
        requestContext.getUriInfo().getRequestUri(),
        payload,
        truncated ? "...(truncated)" : "");
  }

  private static boolean hasEntityMethod(String method) {
    if (method == null) {
      return false;
    }
    return "POST".equals(method) || "PUT".equals(method) || "PATCH".equals(method);
  }
}

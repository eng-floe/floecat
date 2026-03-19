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

package ai.floedb.floecat.gateway.iceberg.rest.support;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CommitTrafficLogger {
  private static final Logger LOG = Logger.getLogger(CommitTrafficLogger.class);
  private static final int MAX_LOG_BYTES = 64 * 1024;

  @ConfigProperty(name = "floecat.rest.log-commit-traffic", defaultValue = "false")
  boolean logCommitTraffic;

  @Inject ObjectMapper objectMapper;

  public boolean enabled() {
    return logCommitTraffic;
  }

  public void logRequest(String method, String path, Object payload) {
    if (!logCommitTraffic) {
      return;
    }
    LOG.infof(
        "REST request method=%s path=%s payload=%s", method, path, serializeAndTruncate(payload));
  }

  public void logResponse(String method, String path, int status, Object payload) {
    if (!logCommitTraffic) {
      return;
    }
    LOG.infof(
        "REST response method=%s path=%s status=%d payload=%s",
        method, path, status, serializeAndTruncate(payload));
  }

  private String serializeAndTruncate(Object payload) {
    if (payload == null) {
      return "<empty>";
    }
    if (payload instanceof String s) {
      return truncateUtf8(s, MAX_LOG_BYTES);
    }
    try {
      return truncateUtf8(objectMapper.writeValueAsString(payload), MAX_LOG_BYTES);
    } catch (JsonProcessingException e) {
      return truncateUtf8(String.valueOf(payload), MAX_LOG_BYTES);
    }
  }

  private static String truncateUtf8(String value, int limitBytes) {
    if (value == null) {
      return "<empty>";
    }
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    if (bytes.length <= limitBytes) {
      return value;
    }
    return new String(bytes, 0, limitBytes, StandardCharsets.UTF_8) + "...(truncated)";
  }
}

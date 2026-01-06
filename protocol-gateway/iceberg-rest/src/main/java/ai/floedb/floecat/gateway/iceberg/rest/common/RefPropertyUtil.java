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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jboss.logging.Logger;

public final class RefPropertyUtil {

  private static final Logger LOG = Logger.getLogger(RefPropertyUtil.class);
  public static final String PROPERTY_KEY = "metadata.refs";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Map<String, Object>>> TYPE =
      new TypeReference<>() {};

  private RefPropertyUtil() {}

  public static Map<String, Map<String, Object>> decode(String value) {
    if (value == null || value.isBlank()) {
      return new LinkedHashMap<>();
    }
    try {
      Map<String, Map<String, Object>> parsed = MAPPER.readValue(value, TYPE);
      return parsed == null ? new LinkedHashMap<>() : new LinkedHashMap<>(parsed);
    } catch (IOException e) {
      LOG.warnf(e, "Failed to parse metadata refs property");
      return new LinkedHashMap<>();
    }
  }

  public static String encode(Map<String, Map<String, Object>> refs) {
    if (refs == null || refs.isEmpty()) {
      return null;
    }
    try {
      return MAPPER.writeValueAsString(refs);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to encode metadata refs property", e);
    }
  }
}

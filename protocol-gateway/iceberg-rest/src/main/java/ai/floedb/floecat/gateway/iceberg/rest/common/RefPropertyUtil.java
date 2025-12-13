package ai.floedb.floecat.gateway.iceberg.rest.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jboss.logging.Logger;

/**
 * Utility for storing and retrieving Iceberg ref metadata in table properties.
 *
 * <p>The backing store is a JSON map keyed by ref-name with values that mirror the Iceberg metadata
 * fields (snapshot-id, type, etc).
 */
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

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

package ai.floedb.floecat.gateway.iceberg.rest.services.compat;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Locale;
import java.util.Map;

@ApplicationScoped
public class TableFormatSupport {

  public TableFormat resolvedFormat(Table table) {
    if (table == null) {
      return TableFormat.TF_UNSPECIFIED;
    }
    if (table.hasUpstream()) {
      TableFormat upstreamFormat = table.getUpstream().getFormat();
      if (upstreamFormat != TableFormat.TF_UNSPECIFIED) {
        return upstreamFormat;
      }
    }
    return parseFromProperties(table.getPropertiesMap());
  }

  public boolean isDelta(Table table) {
    return resolvedFormat(table) == TableFormat.TF_DELTA;
  }

  private TableFormat parseFromProperties(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return TableFormat.TF_UNSPECIFIED;
    }
    String raw = firstNonBlank(properties.get("upstream.format"), properties.get("format"));
    if (raw == null) {
      return TableFormat.TF_UNSPECIFIED;
    }
    String normalized = raw.trim().toUpperCase(Locale.ROOT);
    return switch (normalized) {
      case "DELTA", "TF_DELTA" -> TableFormat.TF_DELTA;
      case "ICEBERG", "TF_ICEBERG" -> TableFormat.TF_ICEBERG;
      default -> TableFormat.TF_UNKNOWN;
    };
  }

  private static String firstNonBlank(String primary, String fallback) {
    if (primary != null && !primary.isBlank()) {
      return primary;
    }
    if (fallback != null && !fallback.isBlank()) {
      return fallback;
    }
    return null;
  }
}

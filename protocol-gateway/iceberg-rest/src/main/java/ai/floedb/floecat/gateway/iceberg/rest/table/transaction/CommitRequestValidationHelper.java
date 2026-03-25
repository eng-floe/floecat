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

package ai.floedb.floecat.gateway.iceberg.rest.table.transaction;

import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@ApplicationScoped
class CommitRequestValidationHelper {
  Response validateTableIdentifier(TableIdentifierDto identifier) {
    if (identifier == null || identifier.name() == null || identifier.name().isBlank()) {
      return IcebergErrorResponses.validation("table identifier is required");
    }
    List<String> namespace = identifier.namespace();
    if (namespace == null) {
      return null;
    }
    for (String segment : namespace) {
      if (segment == null || segment.isBlank()) {
        return IcebergErrorResponses.validation(
            "table identifier namespace entries must be non-blank");
      }
    }
    return null;
  }

  String canonicalTableIdentifier(TableIdentifierDto identifier) {
    List<String> namespacePath = new ArrayList<>();
    if (identifier.namespace() != null) {
      for (String segment : identifier.namespace()) {
        namespacePath.add(segment.trim().toLowerCase(Locale.ROOT));
      }
    }
    String name = identifier.name().trim().toLowerCase(Locale.ROOT);
    return namespacePath.isEmpty() ? name : String.join(".", namespacePath) + "." + name;
  }

  String unsupportedUpdateAction(List<Map<String, Object>> updates) {
    if (updates == null) {
      return null;
    }
    return unsupportedParsedUpdateAction(
        updates.stream()
            .map(update -> new ParsedUpdate(CommitUpdateInspector.actionTypeOf(update), update))
            .toList());
  }

  String unsupportedUpdateAction(ParsedCommit commit) {
    if (commit == null) {
      return null;
    }
    return unsupportedParsedUpdateAction(commit.updateEntries());
  }

  String unsupportedParsedUpdateAction(List<ParsedUpdate> updates) {
    if (updates == null) {
      return null;
    }
    for (ParsedUpdate update : updates) {
      if (update == null || update.rawUpdate() == null) {
        return "<missing>";
      }
      String action = CommitUpdateInspector.actionOf(update.rawUpdate());
      if (action == null || action.isBlank()) {
        return "<missing>";
      }
      if (update.action() == null || !CommitUpdateInspector.isSupportedUpdateAction(action)) {
        return action;
      }
    }
    return null;
  }
}

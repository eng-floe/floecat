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

package ai.floedb.floecat.gateway.iceberg.rest.services.view;

import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.ViewMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.ViewRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.ViewRequests.ViewRepresentation;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.ViewRequests.ViewVersion;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@ApplicationScoped
public class ViewMetadataService {
  public static final String METADATA_PROPERTY_KEY = "view.metadata.json";
  public static final String METADATA_LOCATION_PROPERTY_KEY = "metadata-location";
  private static final Set<String> RESERVED_PROPERTY_KEYS =
      Set.of(METADATA_PROPERTY_KEY, METADATA_LOCATION_PROPERTY_KEY);

  @Inject ObjectMapper mapper;

  public record MetadataContext(
      ViewMetadataView metadata, Map<String, String> userProperties, String sql) {}

  public MetadataContext fromCreate(
      List<String> namespacePath, String viewName, ViewRequests.Create req) {
    if (req == null) {
      throw new IllegalArgumentException("Create view request body is required");
    }
    if (req.schema() == null || req.schema().isNull()) {
      throw new IllegalArgumentException("schema is required");
    }
    if (req.viewVersion() == null) {
      throw new IllegalArgumentException("view-version is required");
    }
    ViewMetadataView.SchemaSummary schema = parseSchema(req.schema());
    ViewMetadataView.ViewVersion version =
        toMetadataVersion(req.viewVersion(), namespacePath, schema.schemaId());
    String sql = extractSql(version);
    Map<String, String> userProps = sanitizeProperties(req.properties());

    String location = resolveLocation(req.location(), namespacePath, viewName);
    String viewUuid = UUID.randomUUID().toString();
    List<ViewMetadataView.ViewVersion> versions = List.of(version);
    List<ViewMetadataView.ViewHistoryEntry> history =
        List.of(new ViewMetadataView.ViewHistoryEntry(version.versionId(), version.timestampMs()));
    List<ViewMetadataView.SchemaSummary> schemas = List.of(schema);
    ViewMetadataView metadata =
        new ViewMetadataView(
            viewUuid, 1, location, version.versionId(), versions, history, schemas, userProps);
    return new MetadataContext(metadata, userProps, sql);
  }

  public MetadataContext fromView(List<String> namespacePath, String viewName, View view) {
    Map<String, String> props = new LinkedHashMap<>(view.getPropertiesMap());
    Map<String, String> userProps = extractUserProperties(props);
    ViewMetadataView metadata =
        parseStoredMetadata(props, namespacePath, viewName, view, userProps);
    String sql = extractSql(metadata);
    return new MetadataContext(metadata, userProps, sql);
  }

  public MetadataContext applyCommit(
      List<String> namespacePath, MetadataContext current, ViewRequests.Commit req) {
    if (req == null || req.updates() == null || req.updates().isEmpty()) {
      throw new IllegalArgumentException("updates are required");
    }
    enforceRequirements(current.metadata(), req.requirements());

    String viewUuid =
        current.metadata().viewUuid() != null
            ? current.metadata().viewUuid()
            : UUID.randomUUID().toString();
    int formatVersion =
        current.metadata().formatVersion() != null ? current.metadata().formatVersion() : 1;
    String location = nonBlank(current.metadata().location(), defaultLocation(namespacePath, ""));
    int currentVersionId =
        current.metadata().currentVersionId() != null ? current.metadata().currentVersionId() : 0;
    List<ViewMetadataView.ViewVersion> versions =
        new ArrayList<>(
            current.metadata().versions() == null ? List.of() : current.metadata().versions());
    List<ViewMetadataView.ViewHistoryEntry> history =
        new ArrayList<>(
            current.metadata().versionLog() == null ? List.of() : current.metadata().versionLog());
    List<ViewMetadataView.SchemaSummary> schemas =
        new ArrayList<>(
            current.metadata().schemas() == null ? List.of() : current.metadata().schemas());
    Map<String, String> userProps = new LinkedHashMap<>(current.userProperties());
    String sql = current.sql();

    if (versions.isEmpty()) {
      ViewMetadataView.ViewVersion synthesized =
          synthesizedVersion(namespacePath, current.metadata().currentVersionId(), sql);
      versions.add(synthesized);
      history.add(
          new ViewMetadataView.ViewHistoryEntry(
              synthesized.versionId(), synthesized.timestampMs()));
      if (schemas.isEmpty()) {
        schemas.add(
            new ViewMetadataView.SchemaSummary(
                synthesized.schemaId(), "struct", List.of(), List.of()));
      }
      currentVersionId = synthesized.versionId();
    }

    for (JsonNode update : req.updates()) {
      if (update == null || !update.hasNonNull("action")) {
        throw new IllegalArgumentException("Update action is required");
      }
      String action = update.get("action").asText();
      switch (action) {
        case "set-location" -> {
          String newLocation = textValue(update, "location");
          if (newLocation == null || newLocation.isBlank()) {
            throw new IllegalArgumentException("location must be non-empty");
          }
          location = newLocation;
        }
        case "set-properties" -> {
          JsonNode updatesNode = update.get("updates");
          if (updatesNode == null || !updatesNode.isObject()) {
            throw new IllegalArgumentException("updates must be an object");
          }
          Map<String, String> additions =
              mapper.convertValue(updatesNode, new TypeReference<Map<String, String>>() {});
          additions.forEach(
              (key, value) -> {
                if (RESERVED_PROPERTY_KEYS.contains(key)) {
                  throw new IllegalArgumentException(key + " is managed internally");
                }
                if (key != null && value != null) {
                  userProps.put(key, value);
                }
              });
        }
        case "remove-properties" -> {
          JsonNode removals = update.get("removals");
          if (removals != null && removals.isArray()) {
            removals.forEach(
                entry -> {
                  String key = entry.asText();
                  if (RESERVED_PROPERTY_KEYS.contains(key)) {
                    throw new IllegalArgumentException(key + " is managed internally");
                  }
                  userProps.remove(key);
                });
          }
        }
        case "assign-uuid" -> {
          String uuid = textValue(update, "uuid");
          if (uuid == null || uuid.isBlank()) {
            throw new IllegalArgumentException("uuid must be non-empty");
          }
          viewUuid = uuid;
        }
        case "add-schema" -> {
          JsonNode schemaNode = update.get("schema");
          if (schemaNode == null || schemaNode.isNull()) {
            throw new IllegalArgumentException("schema is required for add-schema");
          }
          ViewMetadataView.SchemaSummary schema = parseSchema(schemaNode);
          schemas.removeIf(existing -> existing.schemaId().equals(schema.schemaId()));
          schemas.add(schema);
        }
        case "add-view-version" -> {
          JsonNode versionNode = update.get("view-version");
          if (versionNode == null || versionNode.isNull()) {
            throw new IllegalArgumentException("view-version is required for add-view-version");
          }
          ViewVersion dto = mapper.convertValue(versionNode, ViewVersion.class);
          ViewMetadataView.ViewVersion newVersion =
              toMetadataVersion(dto, namespacePath, dto.schemaId());
          versions.add(newVersion);
          history.add(
              new ViewMetadataView.ViewHistoryEntry(
                  newVersion.versionId(), newVersion.timestampMs()));
          currentVersionId = newVersion.versionId();
          sql = extractSql(newVersion);
        }
        case "set-current-view-version" -> {
          int target = update.has("view-version-id") ? update.get("view-version-id").asInt(-1) : -1;
          if (target == -1) {
            currentVersionId = versions.get(versions.size() - 1).versionId();
          } else {
            boolean exists = versions.stream().anyMatch(v -> v.versionId() == target);
            if (!exists) {
              throw new IllegalArgumentException("view-version-id " + target + " does not exist");
            }
            currentVersionId = target;
          }
          sql = extractSql(versions, currentVersionId, sql);
        }
        default -> throw new IllegalArgumentException("Unsupported view update action: " + action);
      }
    }

    ViewMetadataView metadata =
        new ViewMetadataView(
            viewUuid,
            formatVersion,
            location,
            currentVersionId,
            List.copyOf(versions),
            dedupeHistory(history),
            List.copyOf(schemas),
            Map.copyOf(userProps));
    return new MetadataContext(metadata, Map.copyOf(userProps), sql);
  }

  public MetadataContext withSql(MetadataContext context, String sql) {
    if (sql == null || sql.isBlank()) {
      throw new IllegalArgumentException("sql must be non-empty");
    }
    ViewMetadataView updated = updateCurrentVersionSql(context.metadata(), sql);
    return new MetadataContext(updated, context.userProperties(), sql);
  }

  public MetadataContext withUserProperties(MetadataContext context, Map<String, String> props) {
    Map<String, String> sanitized = sanitizeProperties(props);
    ViewMetadataView updated =
        new ViewMetadataView(
            context.metadata().viewUuid(),
            context.metadata().formatVersion(),
            context.metadata().location(),
            context.metadata().currentVersionId(),
            context.metadata().versions(),
            context.metadata().versionLog(),
            context.metadata().schemas(),
            sanitized);
    return new MetadataContext(updated, sanitized, context.sql());
  }

  public Map<String, String> buildPropertyMap(MetadataContext context) {
    Map<String, String> props = new LinkedHashMap<>(context.userProperties());
    props.put(METADATA_LOCATION_PROPERTY_KEY, context.metadata().location());
    props.put(METADATA_PROPERTY_KEY, serializeMetadata(context.metadata()));
    return props;
  }

  private void enforceRequirements(ViewMetadataView metadata, List<JsonNode> requirements) {
    if (requirements == null || requirements.isEmpty()) {
      return;
    }
    for (JsonNode requirement : requirements) {
      if (requirement == null || !requirement.hasNonNull("type")) {
        throw new IllegalArgumentException("requirement type is required");
      }
      String type = requirement.get("type").asText();
      if ("assert-view-uuid".equals(type)) {
        String requiredUuid = textValue(requirement, "uuid");
        if (requiredUuid == null || requiredUuid.isBlank()) {
          throw new IllegalArgumentException("uuid is required for assert-view-uuid");
        }
        String currentUuid = metadata.viewUuid();
        if (currentUuid != null && !currentUuid.equals(requiredUuid)) {
          throw new IllegalArgumentException("View UUID does not match requirement");
        }
      } else {
        throw new IllegalArgumentException("Unsupported view requirement: " + type);
      }
    }
  }

  private ViewMetadataView.ViewVersion synthesizedVersion(
      List<String> namespacePath, Integer versionId, String sql) {
    int nextId = versionId != null && versionId >= 0 ? versionId : 0;
    long timestamp = Instant.now().toEpochMilli();
    ViewMetadataView.ViewRepresentation representation =
        new ViewMetadataView.ViewRepresentation("sql", sql != null ? sql : "select 1", "ansi");
    return new ViewMetadataView.ViewVersion(
        nextId,
        timestamp,
        0,
        Map.of("operation", "unknown"),
        List.of(representation),
        namespacePath,
        null);
  }

  private ViewMetadataView.ViewVersion toMetadataVersion(
      ViewVersion version, List<String> namespacePath, Integer schemaId) {
    if (version == null) {
      throw new IllegalArgumentException("view-version is required");
    }
    int versionId =
        version.versionId() != null && version.versionId() >= 0 ? version.versionId() : 0;
    long timestamp =
        version.timestampMs() != null && version.timestampMs() > 0
            ? version.timestampMs()
            : Instant.now().toEpochMilli();
    int resolvedSchemaId =
        version.schemaId() != null && version.schemaId() >= 0
            ? version.schemaId()
            : schemaId != null ? schemaId : 0;
    Map<String, String> summary = version.summary() == null ? Map.of() : version.summary();
    List<ViewRepresentation> reps = version.representations();
    if (reps == null || reps.isEmpty()) {
      throw new IllegalArgumentException("view-version.representations is required");
    }
    List<ViewMetadataView.ViewRepresentation> metadataReps = new ArrayList<>();
    for (ViewRepresentation rep : reps) {
      if (rep == null || rep.sql() == null) {
        continue;
      }
      String type = nonBlank(rep.type(), "sql");
      String dialect = nonBlank(rep.dialect(), "ansi");
      metadataReps.add(new ViewMetadataView.ViewRepresentation(type, rep.sql(), dialect));
    }
    if (metadataReps.isEmpty()) {
      throw new IllegalArgumentException("At least one SQL representation is required");
    }
    List<String> defaultNamespace =
        version.defaultNamespace() == null || version.defaultNamespace().isEmpty()
            ? namespacePath
            : version.defaultNamespace();
    String defaultCatalog = nonBlank(version.defaultCatalog(), null);
    return new ViewMetadataView.ViewVersion(
        versionId,
        timestamp,
        resolvedSchemaId,
        summary,
        metadataReps,
        defaultNamespace,
        defaultCatalog);
  }

  private ViewMetadataView.SchemaSummary parseSchema(JsonNode schemaNode) {
    if (schemaNode == null || schemaNode.isNull()) {
      throw new IllegalArgumentException("schema is required");
    }
    Map<String, Object> schemaMap =
        mapper.convertValue(schemaNode, new TypeReference<Map<String, Object>>() {});
    int schemaId = number(schemaMap.getOrDefault("schema-id", 0)).intValue();
    String type = (String) schemaMap.getOrDefault("type", "struct");
    List<Map<String, Object>> fields =
        mapper.convertValue(
            schemaMap.getOrDefault("fields", List.of()),
            new TypeReference<List<Map<String, Object>>>() {});
    List<Integer> identifierFieldIds =
        mapper.convertValue(
            schemaMap.getOrDefault("identifier-field-ids", List.of()),
            new TypeReference<List<Integer>>() {});
    return new ViewMetadataView.SchemaSummary(schemaId, type, fields, identifierFieldIds);
  }

  private ViewMetadataView parseStoredMetadata(
      Map<String, String> props,
      List<String> namespacePath,
      String viewName,
      View view,
      Map<String, String> userProps) {
    String raw = props.get(METADATA_PROPERTY_KEY);
    if (raw != null && !raw.isBlank()) {
      try {
        ViewMetadataView metadata = mapper.readValue(raw, ViewMetadataView.class);
        return new ViewMetadataView(
            metadata.viewUuid(),
            metadata.formatVersion(),
            nonBlank(metadata.location(), props.get(METADATA_LOCATION_PROPERTY_KEY)),
            metadata.currentVersionId(),
            metadata.versions(),
            metadata.versionLog(),
            metadata.schemas(),
            userProps);
      } catch (Exception ignored) {
        // fall through
      }
    }
    return synthesizeMetadata(namespacePath, viewName, view, userProps);
  }

  private ViewMetadataView synthesizeMetadata(
      List<String> namespacePath, String viewName, View view, Map<String, String> userProps) {
    String metadataLocation =
        nonBlank(
            view.getPropertiesOrDefault(METADATA_LOCATION_PROPERTY_KEY, null),
            resolveLocation(null, namespacePath, viewName));
    long timestamp =
        view.hasCreatedAt()
            ? view.getCreatedAt().getSeconds() * 1000 + view.getCreatedAt().getNanos() / 1_000_000
            : Instant.now().toEpochMilli();
    ViewMetadataView.ViewRepresentation representation =
        new ViewMetadataView.ViewRepresentation("sql", nonBlank(view.getSql(), "select 1"), "ansi");
    ViewMetadataView.ViewVersion version =
        new ViewMetadataView.ViewVersion(
            0,
            timestamp,
            0,
            Map.of("operation", "unknown"),
            List.of(representation),
            namespacePath,
            null);
    ViewMetadataView.ViewHistoryEntry history = new ViewMetadataView.ViewHistoryEntry(0, timestamp);
    ViewMetadataView.SchemaSummary schema =
        new ViewMetadataView.SchemaSummary(0, "struct", List.of(), List.of());
    return new ViewMetadataView(
        view.hasResourceId() ? view.getResourceId().getId() : viewName,
        1,
        metadataLocation,
        0,
        List.of(version),
        List.of(history),
        List.of(schema),
        userProps);
  }

  private ViewMetadataView updateCurrentVersionSql(ViewMetadataView metadata, String sql) {
    List<ViewMetadataView.ViewVersion> versions =
        new ArrayList<>(metadata.versions() == null ? List.of() : metadata.versions());
    if (versions.isEmpty()) {
      versions.add(
          new ViewMetadataView.ViewVersion(
              metadata.currentVersionId(),
              Instant.now().toEpochMilli(),
              0,
              Map.of("operation", "update"),
              List.of(new ViewMetadataView.ViewRepresentation("sql", sql, "ansi")),
              List.of(),
              null));
    } else {
      int idx = versions.size() - 1;
      for (int i = 0; i < versions.size(); i++) {
        if (versions.get(i).versionId() == metadata.currentVersionId()) {
          idx = i;
          break;
        }
      }
      ViewMetadataView.ViewVersion target = versions.get(idx);
      List<ViewMetadataView.ViewRepresentation> reps =
          new ArrayList<>(target.representations() == null ? List.of() : target.representations());
      if (reps.isEmpty()) {
        reps.add(new ViewMetadataView.ViewRepresentation("sql", sql, "ansi"));
      } else {
        ViewMetadataView.ViewRepresentation original = reps.get(0);
        reps.set(
            0,
            new ViewMetadataView.ViewRepresentation(
                nonBlank(original.type(), "sql"), sql, nonBlank(original.dialect(), "ansi")));
      }
      versions.set(
          idx,
          new ViewMetadataView.ViewVersion(
              target.versionId(),
              target.timestampMs(),
              target.schemaId(),
              target.summary(),
              List.copyOf(reps),
              target.defaultNamespace(),
              target.defaultCatalog()));
    }
    return new ViewMetadataView(
        metadata.viewUuid(),
        metadata.formatVersion(),
        metadata.location(),
        metadata.currentVersionId(),
        List.copyOf(versions),
        metadata.versionLog(),
        metadata.schemas(),
        metadata.properties());
  }

  private List<ViewMetadataView.ViewHistoryEntry> dedupeHistory(
      List<ViewMetadataView.ViewHistoryEntry> entries) {
    Set<Integer> ids = new LinkedHashSet<>();
    List<ViewMetadataView.ViewHistoryEntry> deduped = new ArrayList<>();
    for (ViewMetadataView.ViewHistoryEntry entry : entries) {
      if (entry == null || entry.versionId() == null) {
        continue;
      }
      if (ids.add(entry.versionId())) {
        deduped.add(entry);
      }
    }
    return deduped;
  }

  private Map<String, String> sanitizeProperties(Map<String, String> props) {
    Map<String, String> sanitized = new LinkedHashMap<>();
    if (props == null) {
      return sanitized;
    }
    props.forEach(
        (key, value) -> {
          if (key == null || value == null || RESERVED_PROPERTY_KEYS.contains(key)) {
            return;
          }
          sanitized.put(key, value);
        });
    return sanitized;
  }

  private Map<String, String> extractUserProperties(Map<String, String> props) {
    Map<String, String> user = new LinkedHashMap<>(props);
    RESERVED_PROPERTY_KEYS.forEach(user::remove);
    user.values().removeIf(v -> v == null);
    return user;
  }

  private String serializeMetadata(ViewMetadataView metadata) {
    try {
      return mapper.writeValueAsString(metadata);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to serialize view metadata", e);
    }
  }

  private String resolveLocation(String location, List<String> namespacePath, String viewName) {
    if (location != null && !location.isBlank()) {
      return location;
    }
    return defaultLocation(namespacePath, viewName);
  }

  private String defaultLocation(List<String> namespacePath, String viewName) {
    String ns =
        namespacePath == null || namespacePath.isEmpty() ? "" : String.join("/", namespacePath);
    if (ns.isBlank()) {
      return "floecat://views/" + viewName + "/metadata.json";
    }
    return "floecat://views/" + ns + "/" + viewName + "/metadata.json";
  }

  private String extractSql(ViewMetadataView metadata) {
    if (metadata == null || metadata.versions() == null || metadata.versions().isEmpty()) {
      return "select 1";
    }
    return extractSql(metadata.versions(), metadata.currentVersionId(), "select 1");
  }

  private String extractSql(
      List<ViewMetadataView.ViewVersion> versions, Integer versionId, String fallback) {
    if (versions == null || versions.isEmpty()) {
      return fallback;
    }
    ViewMetadataView.ViewVersion target =
        versions.stream()
            .filter(v -> v.versionId().equals(versionId))
            .findFirst()
            .orElseGet(() -> versions.get(versions.size() - 1));
    List<ViewMetadataView.ViewRepresentation> reps =
        target.representations() == null ? List.of() : target.representations();
    return reps.isEmpty() ? fallback : reps.get(0).sql();
  }

  private String extractSql(ViewMetadataView.ViewVersion version) {
    if (version == null
        || version.representations() == null
        || version.representations().isEmpty()) {
      return "select 1";
    }
    return version.representations().get(0).sql();
  }

  private String textValue(JsonNode node, String field) {
    return node.has(field) && !node.get(field).isNull() ? node.get(field).asText() : null;
  }

  private Number number(Object value) {
    if (value instanceof Number num) {
      return num;
    }
    if (value instanceof String str && !str.isBlank()) {
      return Double.valueOf(str);
    }
    return 0;
  }

  private String nonBlank(String value, String fallback) {
    return value != null && !value.isBlank() ? value : fallback;
  }
}

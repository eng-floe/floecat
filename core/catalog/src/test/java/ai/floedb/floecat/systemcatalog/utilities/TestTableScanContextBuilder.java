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

package ai.floedb.floecat.systemcatalog.utilities;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.*;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.time.Instant;
import java.util.*;

/** Test builder for table- and namespace-based scanners (information_schema.*). */
public final class TestTableScanContextBuilder extends AbstractTestScanContextBuilder {

  private TestTableScanContextBuilder(ResourceId catalogId) {
    super(catalogId);
    CatalogNode catalog =
        new CatalogNode(
            catalogId,
            1,
            Instant.EPOCH,
            catalogId.getId(),
            Map.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of());
    overlay.addNode(catalog);
  }

  public static TestTableScanContextBuilder builder(String catalogName) {
    return new TestTableScanContextBuilder(
        ResourceId.newBuilder()
            .setAccountId("account")
            .setKind(ResourceKind.RK_CATALOG)
            .setId(catalogName)
            .build());
  }

  public NamespaceNode addNamespace(String dottedPath) {
    List<String> segments = List.of(dottedPath.split("\\."));
    List<String> path = segments.size() > 1 ? segments.subList(0, segments.size() - 1) : List.of();

    String display = segments.get(segments.size() - 1);

    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId(catalogId.getAccountId())
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId(dottedPath)
            .build();

    NamespaceNode ns =
        new NamespaceNode(
            id,
            1,
            Instant.EPOCH,
            catalogId,
            path,
            display,
            GraphNodeOrigin.USER,
            Map.of(),
            Map.of());

    overlay.addNode(ns);
    return ns;
  }

  public UserTableNode addTable(
      NamespaceNode ns, String name, Map<String, Integer> fieldIds, Map<String, String> types) {

    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(catalogId.getAccountId())
            .setKind(ResourceKind.RK_TABLE)
            .setId(ns.id().getId() + "." + name)
            .build();

    UserTableNode table =
        new UserTableNode(
            tableId,
            1,
            Instant.EPOCH,
            catalogId,
            ns.id(),
            name,
            TableFormat.TF_ICEBERG,
            ColumnIdAlgorithm.CID_FIELD_ID,
            "",
            Map.of(),
            List.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            List.of(),
            Map.of(),
            Map.of());

    overlay.addRelation(ns.id(), table);
    overlay.setTableSchema(tableId, buildSchema(fieldIds, types));
    return table;
  }

  private static List<SchemaColumn> buildSchema(
      Map<String, Integer> fieldIds, Map<String, String> types) {

    List<SchemaColumn> schema = new ArrayList<>();

    // Sort by field id (Iceberg / Delta semantics)
    fieldIds.entrySet().stream()
        .sorted(Map.Entry.comparingByValue())
        .forEach(
            e -> {
              String name = e.getKey();
              Integer fieldId = e.getValue();
              String type = types.get(name);

              SchemaColumn.Builder b =
                  SchemaColumn.newBuilder().setName(simpleName(name)).setFieldId(fieldId);

              if (type != null) {
                b.setLogicalType(type);
              }

              // default nullability for tests
              b.setNullable(true);

              schema.add(b.build());
            });

    return schema;
  }

  private static String simpleName(String path) {
    int idx = path.lastIndexOf('.');
    return idx < 0 ? path : path.substring(idx + 1);
  }
}

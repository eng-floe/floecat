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

package ai.floedb.floecat.connector.iceberg.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ReplaceViewVersion;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.UpdateViewProperties;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewHistoryEntry;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.iceberg.view.ViewVersion;
import org.junit.jupiter.api.Test;

class IcebergViewConnectorTest {

  @Test
  void restConnectorListsViewDescriptorsFromCatalog() {
    Namespace namespace = Namespace.of("ns1", "ns2");
    FakeRestCatalog catalog = new FakeRestCatalog();
    catalog.addView(namespace, "v_b", fakeView("v_b", "SELECT 2", "trino", namespace));
    catalog.addView(namespace, "v_a", fakeView("v_a", "SELECT 1", "spark", namespace));

    var connector = new IcebergRestConnector("iceberg-rest", catalog, false, 0.1d, 0L);

    List<FloecatConnector.ViewDescriptor> views = connector.listViewDescriptors("ns1.ns2");

    assertThat(views).hasSize(2);
    assertThat(views.get(0).name()).isEqualTo("v_a");
    assertThat(views.get(0).sql()).isEqualTo("SELECT 1");
    assertThat(views.get(0).dialect()).isEqualTo("spark");
    assertThat(views.get(0).searchPath()).containsExactly("ns1", "ns2");
    assertThat(views.get(0).schemaJson()).contains("\"id\"");
    assertThat(views.get(1).name()).isEqualTo("v_b");
    assertThat(views.get(1).dialect()).isEqualTo("trino");
  }

  @Test
  void restConnectorDescribeViewFallsBackToFirstSqlRepresentation() {
    Namespace namespace = Namespace.of("db");
    FakeRestCatalog catalog = new FakeRestCatalog();
    catalog.addView(
        namespace,
        "v_only",
        fakeViewWithoutSpark("v_only", "SELECT current_date", "trino", namespace));

    var connector = new IcebergRestConnector("iceberg-rest", catalog, false, 0.1d, 0L);

    Optional<FloecatConnector.ViewDescriptor> described = connector.describeView("db", "v_only");

    assertThat(described).isPresent();
    assertThat(described.orElseThrow().sql()).isEqualTo("SELECT current_date");
    assertThat(described.orElseThrow().dialect()).isEqualTo("trino");
    assertThat(described.orElseThrow().searchPath()).containsExactly("db");
  }

  private static View fakeView(
      String name, String sql, String dialect, Namespace defaultNamespace) {
    return new FakeView(name, sql, dialect, defaultNamespace, true);
  }

  private static View fakeViewWithoutSpark(
      String name, String sql, String dialect, Namespace defaultNamespace) {
    return new FakeView(name, sql, dialect, defaultNamespace, false);
  }

  private static final class FakeRestCatalog extends RESTCatalog {
    private final Map<Namespace, Map<String, View>> views = new HashMap<>();

    void addView(Namespace namespace, String name, View view) {
      views.computeIfAbsent(namespace, ignored -> new HashMap<>()).put(name, view);
    }

    @Override
    public List<TableIdentifier> listViews(Namespace namespace) {
      return views.getOrDefault(namespace, Map.of()).keySet().stream()
          .map(name -> TableIdentifier.of(namespace, name))
          .toList();
    }

    @Override
    public View loadView(TableIdentifier identifier) {
      Map<String, View> namespaceViews = views.get(identifier.namespace());
      if (namespaceViews == null) {
        throw new IllegalArgumentException("missing namespace");
      }
      View view = namespaceViews.get(identifier.name());
      if (view == null) {
        throw new IllegalArgumentException("missing view");
      }
      return view;
    }
  }

  private static final class FakeView implements View {
    private final String name;
    private final Schema schema =
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    private final ViewVersion version;
    private final SQLViewRepresentation representation;
    private final boolean includeSparkAccessor;

    FakeView(
        String name,
        String sql,
        String dialect,
        Namespace defaultNamespace,
        boolean includeSparkAccessor) {
      this.name = name;
      this.representation = new FakeSqlViewRepresentation(sql, dialect);
      this.version = new FakeViewVersion(defaultNamespace, List.of(representation));
      this.includeSparkAccessor = includeSparkAccessor;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public Map<Integer, Schema> schemas() {
      return Map.of(schema.schemaId(), schema);
    }

    @Override
    public ViewVersion currentVersion() {
      return version;
    }

    @Override
    public Iterable<ViewVersion> versions() {
      return List.of(version);
    }

    @Override
    public ViewVersion version(int versionId) {
      return version;
    }

    @Override
    public List<ViewHistoryEntry> history() {
      return List.of();
    }

    @Override
    public Map<String, String> properties() {
      return Map.of();
    }

    @Override
    public UpdateViewProperties updateProperties() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ReplaceViewVersion replaceVersion() {
      throw new UnsupportedOperationException();
    }

    @Override
    public UpdateLocation updateLocation() {
      throw new UnsupportedOperationException();
    }

    @Override
    public UUID uuid() {
      return new UUID(0L, 0L);
    }

    @Override
    public SQLViewRepresentation sqlFor(String dialect) {
      if (!includeSparkAccessor) {
        return null;
      }
      return "spark".equals(dialect) ? representation : null;
    }
  }

  private record FakeViewVersion(
      Namespace defaultNamespace, List<ViewRepresentation> representations) implements ViewVersion {
    @Override
    public int versionId() {
      return 1;
    }

    @Override
    public long timestampMillis() {
      return 0L;
    }

    @Override
    public Map<String, String> summary() {
      return Map.of();
    }

    @Override
    public int schemaId() {
      return 0;
    }
  }

  private record FakeSqlViewRepresentation(String sql, String dialect)
      implements SQLViewRepresentation {
    @Override
    public String type() {
      return "sql";
    }
  }
}

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

package ai.floedb.floecat.connector.delta.uc.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.connector.spi.AuthProvider;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UnityDeltaConnectorTest {

  private HttpServer server;
  private UnityDeltaConnector connector;

  private static final AuthProvider NO_AUTH =
      new AuthProvider() {
        @Override
        public String scheme() {
          return "none";
        }

        @Override
        public Map<String, String> apply(Map<String, String> baseProps) {
          return baseProps;
        }
      };

  @BeforeEach
  void setUp() throws Exception {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.setExecutor(null);
    server.start();
    int port = server.getAddress().getPort();
    UcHttp http = new UcHttp("http://localhost:" + port, 1000, 5000, NO_AUTH);
    connector = new UnityDeltaConnector("test-id", http, null, null, null, false, 0.0, 0);
  }

  @AfterEach
  void tearDown() {
    server.stop(0);
  }

  @Test
  void describeThrowsOnNotFound() throws Exception {
    server.createContext(
        "/api/2.1/unity-catalog/tables/",
        exchange -> {
          String body = "{\"error_code\":\"RESOURCE_DOES_NOT_EXIST\",\"message\":\"Not found\"}";
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(404, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.getResponseBody().close();
        });

    assertThatThrownBy(() -> connector.describe("mycat.myschema", "missing_table"))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("404");
  }

  @Test
  void listViewsReturnsOnlyViewTableTypes() throws Exception {
    server.createContext(
        "/api/2.1/unity-catalog/tables",
        exchange -> {
          String body =
              """
              {"tables":[
                {"name":"sales_view","table_type":"VIEW"},
                {"name":"orders","data_source_format":"DELTA"},
                {"name":"summary_view","table_type":"VIEW"}
              ]}
              """;
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.getResponseBody().close();
        });

    List<String> views = connector.listViews("mycat.myschema");

    // Only VIEW table_type entries are returned; sorted alphabetically.
    assertThat(views).containsExactly("sales_view", "summary_view");
  }

  @Test
  void listViewsReturnsEmptyWhenNamespaceHasNoDot() {
    // No HTTP call expected since the namespace is malformed.
    List<String> views = connector.listViews("nodot");
    assertThat(views).isEmpty();
  }

  @Test
  void listViewsIgnoresDeltaTables() throws Exception {
    server.createContext(
        "/api/2.1/unity-catalog/tables",
        exchange -> {
          String body =
              """
              {"tables":[
                {"name":"orders","data_source_format":"DELTA"},
                {"name":"customers","data_source_format":"DELTA"}
              ]}
              """;
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.getResponseBody().close();
        });

    List<String> views = connector.listViews("mycat.myschema");
    assertThat(views).isEmpty();
  }

  @Test
  void describeViewReturnsViewDescriptorWithSqlAndColumns() throws Exception {
    server.createContext(
        "/api/2.1/unity-catalog/tables/",
        exchange -> {
          String body =
              """
              {
                "name":"revenue_view",
                "table_type":"VIEW",
                "view_definition":"SELECT amount, region FROM sales",
                "columns":[
                  {"name":"amount","type_name":"DOUBLE","nullable":false},
                  {"name":"region","type_name":"STRING","nullable":true}
                ]
              }
              """;
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.getResponseBody().close();
        });

    Optional<FloecatConnector.ViewDescriptor> result =
        connector.describeView("mycat.myschema", "revenue_view");

    assertThat(result).isPresent();
    FloecatConnector.ViewDescriptor view = result.get();
    assertThat(view.sql()).isEqualTo("SELECT amount, region FROM sales");
    assertThat(view.dialect()).isEqualTo("spark");
    assertThat(view.namespaceFq()).isEqualTo("mycat.myschema");
    assertThat(view.name()).isEqualTo("revenue_view");
    // searchPath must contain only the schema segment — catalog is handled separately.
    assertThat(view.searchPath()).containsExactly("myschema");
    // schemaJson must contain the column names
    assertThat(view.schemaJson()).contains("\"amount\"");
    assertThat(view.schemaJson()).contains("\"region\"");
  }

  @Test
  void describeViewUsesTypeNameFallbackWhenTypeTextMissing() throws Exception {
    server.createContext(
        "/api/2.1/unity-catalog/tables/",
        exchange -> {
          String body =
              """
              {
                "name":"typed_view",
                "table_type":"VIEW",
                "view_definition":"SELECT id FROM t",
                "columns":[
                  {"name":"id","type_name":"INT","nullable":false}
                ]
              }
              """;
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.getResponseBody().close();
        });

    Optional<FloecatConnector.ViewDescriptor> result =
        connector.describeView("cat.schema", "typed_view");

    assertThat(result).isPresent();
    assertThat(result.get().sql()).isEqualTo("SELECT id FROM t");
    // type_name "INT" should appear in schema JSON
    assertThat(result.get().schemaJson()).contains("\"id\"");
    assertThat(result.get().schemaJson()).contains("INT");
  }

  @Test
  void describeViewReturnsEmptyOnNotFound() throws Exception {
    server.createContext(
        "/api/2.1/unity-catalog/tables/",
        exchange -> {
          // Simulate UC returning 404 (view deleted between list and describe)
          String body = "{\"error_code\":\"RESOURCE_DOES_NOT_EXIST\",\"message\":\"Not found\"}";
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(404, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.getResponseBody().close();
        });

    Optional<FloecatConnector.ViewDescriptor> result =
        connector.describeView("mycat.myschema", "gone_view");

    assertThat(result).isEmpty();
  }

  @Test
  void listViewDescriptorsBatchesAllViewsInSingleCall() throws Exception {
    // listViewDescriptors() must call the list endpoint only once and return full descriptors.
    server.createContext(
        "/api/2.1/unity-catalog/tables",
        exchange -> {
          String body =
              """
              {"tables":[
                {"name":"v1","table_type":"VIEW","view_definition":"SELECT a FROM t1",
                 "columns":[{"name":"a","type_text":"INT","nullable":true}]},
                {"name":"orders","data_source_format":"DELTA"},
                {"name":"v2","table_type":"VIEW","view_definition":"SELECT b FROM t2",
                 "columns":[{"name":"b","type_text":"STRING","nullable":false}]}
              ]}
              """;
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.getResponseBody().close();
        });

    List<FloecatConnector.ViewDescriptor> views = connector.listViewDescriptors("mycat.myschema");

    // Only VIEW entries; sorted alphabetically.
    assertThat(views).hasSize(2);
    assertThat(views.get(0).name()).isEqualTo("v1");
    assertThat(views.get(0).sql()).isEqualTo("SELECT a FROM t1");
    assertThat(views.get(0).dialect()).isEqualTo("spark");
    assertThat(views.get(0).schemaJson()).contains("\"a\"");
    assertThat(views.get(1).name()).isEqualTo("v2");
    assertThat(views.get(1).sql()).isEqualTo("SELECT b FROM t2");
  }

  @Test
  void listViewDescriptorsReturnsEmptyWhenNamespaceHasNoDot() {
    // No HTTP call expected since the namespace is malformed.
    List<FloecatConnector.ViewDescriptor> views = connector.listViewDescriptors("nodot");
    assertThat(views).isEmpty();
  }

  @Test
  void listViewDescriptorsSearchPathContainsOnlySchemaSegments() throws Exception {
    // Verifies fix for issue 3: searchPath must NOT include the catalog prefix.
    // For "mycat.myschema", searchPath should be ["myschema"], not ["mycat", "myschema"].
    // Including the catalog causes enrichForViewContext to prepend it a second time, producing
    // catalog.catalog.schema.table resolution for unqualified base relation names.
    server.createContext(
        "/api/2.1/unity-catalog/tables",
        exchange -> {
          String body =
              """
              {"tables":[
                {"name":"v1","table_type":"VIEW","view_definition":"SELECT x FROM t",
                 "columns":[{"name":"x","type_text":"INT","nullable":true}]}
              ]}
              """;
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.getResponseBody().close();
        });

    List<FloecatConnector.ViewDescriptor> views = connector.listViewDescriptors("mycat.myschema");

    assertThat(views).hasSize(1);
    assertThat(views.get(0).searchPath()).containsExactly("myschema");
  }

  @Test
  void listViewDescriptorsSearchPathPreservesMultiLevelSchema() throws Exception {
    // For a three-segment namespace "mycat.ns1.ns2", searchPath should be ["ns1", "ns2"].
    server.createContext(
        "/api/2.1/unity-catalog/tables",
        exchange -> {
          String body =
              """
              {"tables":[
                {"name":"v1","table_type":"VIEW","view_definition":"SELECT x FROM t",
                 "columns":[{"name":"x","type_text":"INT","nullable":true}]}
              ]}
              """;
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.getResponseBody().close();
        });

    List<FloecatConnector.ViewDescriptor> views = connector.listViewDescriptors("mycat.ns1.ns2");

    assertThat(views).hasSize(1);
    assertThat(views.get(0).searchPath()).containsExactly("ns1", "ns2");
  }

  // -------------------------------------------------------------------------
  // Pagination tests
  // -------------------------------------------------------------------------

  @Test
  void listTablesPaginatesAcrossMultiplePages() throws Exception {
    // UC returns two pages: first has next_page_token, second does not.
    server.createContext(
        "/api/2.1/unity-catalog/tables",
        exchange -> {
          String query = exchange.getRequestURI().getQuery();
          String body =
              (query != null && query.contains("page_token=page2"))
                  ? """
                    {"tables":[
                      {"name":"t2","data_source_format":"DELTA"}
                    ]}
                    """
                  : """
                    {"tables":[
                      {"name":"t1","data_source_format":"DELTA"}
                    ],"next_page_token":"page2"}
                    """;
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.getResponseBody().close();
        });

    List<String> tables = connector.listTables("mycat.myschema");
    assertThat(tables).containsExactly("t1", "t2");
  }

  @Test
  void listNamespacesPaginatesCatalogs() throws Exception {
    // Catalogs endpoint returns two pages; schemas endpoint returns a single page per catalog.
    server.createContext(
        "/api/2.1/unity-catalog/catalogs",
        exchange -> {
          String query = exchange.getRequestURI().getQuery();
          String body =
              (query != null && query.contains("page_token=cp2"))
                  ? """
                    {"catalogs":[{"name":"cat2"}]}
                    """
                  : """
                    {"catalogs":[{"name":"cat1"}],"next_page_token":"cp2"}
                    """;
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.getResponseBody().close();
        });
    server.createContext(
        "/api/2.1/unity-catalog/schemas",
        exchange -> {
          // Determine catalog from query and return one schema per catalog.
          String query = exchange.getRequestURI().getQuery();
          String schemaName = (query != null && query.contains("cat2")) ? "s2" : "s1";
          String body = "{\"schemas\":[{\"name\":\"" + schemaName + "\"}]}";
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.getResponseBody().close();
        });

    List<String> namespaces = connector.listNamespaces();
    assertThat(namespaces).containsExactlyInAnyOrder("cat1.s1", "cat2.s2");
  }

  @Test
  void listNamespacesPaginatesSchemasWithinCatalog() throws Exception {
    // Schemas endpoint returns two pages for the same catalog.
    server.createContext(
        "/api/2.1/unity-catalog/catalogs",
        exchange -> {
          String body = "{\"catalogs\":[{\"name\":\"mycat\"}]}";
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.getResponseBody().close();
        });
    server.createContext(
        "/api/2.1/unity-catalog/schemas",
        exchange -> {
          String query = exchange.getRequestURI().getQuery();
          String body =
              (query != null && query.contains("page_token=sp2"))
                  ? """
                    {"schemas":[{"name":"ns2"}]}
                    """
                  : """
                    {"schemas":[{"name":"ns1"}],"next_page_token":"sp2"}
                    """;
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.getResponseBody().close();
        });

    List<String> namespaces = connector.listNamespaces();
    assertThat(namespaces).containsExactlyInAnyOrder("mycat.ns1", "mycat.ns2");
  }
}

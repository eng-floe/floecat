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
    assertThat(view.searchPath()).containsExactly("mycat", "myschema");
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
}

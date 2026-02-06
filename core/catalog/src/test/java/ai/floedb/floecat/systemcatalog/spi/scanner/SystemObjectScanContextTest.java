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

package ai.floedb.floecat.systemcatalog.spi.scanner;

import static org.assertj.core.api.Assertions.*;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.*;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SystemObjectScanContextTest {

  @Test
  void resolve_throws_whenMissing() {
    CatalogOverlay view = new TestCatalogOverlay();

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view,
            NameRef.newBuilder().build(),
            ResourceId.getDefaultInstance(),
            EngineContext.empty());

    assertThatThrownBy(() -> ctx.resolve(ResourceId.getDefaultInstance()))
        .isInstanceOf(Exception.class);
  }

  @Test
  void tryResolve_returnsEmptyWhenMissing() {
    CatalogOverlay view = new TestCatalogOverlay();

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view,
            NameRef.newBuilder().build(),
            ResourceId.getDefaultInstance(),
            EngineContext.empty());

    assertThat(ctx.tryResolve(ResourceId.getDefaultInstance())).isEmpty();
  }

  @Test
  void tryResolve_returnsNodeWhenPresent() {
    GraphNode node =
        new UserTableNode(
            ResourceId.getDefaultInstance(),
            0L,
            Instant.EPOCH,
            ResourceId.getDefaultInstance(),
            ResourceId.getDefaultInstance(),
            "t",
            null, // format (not relevant for this test)
            null, // columnIdAlgorithm (not relevant for this test)
            "{}", // schemaJson
            Map.of(), // properties
            List.of(), // partitionKeys
            Optional.empty(), // currentSnapshot
            Optional.empty(), // previousSnapshot
            Optional.empty(), // resolvedSnapshots
            Optional.empty(), // statsSummary
            List.of(), // dependentViews
            Map.of(), // engineHints
            Map.of()); // columnHints

    TestCatalogOverlay view = new TestCatalogOverlay();
    view.addNode(node);

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view,
            NameRef.getDefaultInstance(),
            ResourceId.getDefaultInstance(),
            EngineContext.empty());

    assertThat(ctx.tryResolve(ResourceId.getDefaultInstance())).isPresent().containsSame(node);
  }

  @Test
  void resolve_returnsNodeWhenPresent() {
    GraphNode node =
        new UserTableNode(
            ResourceId.getDefaultInstance(),
            0L,
            Instant.EPOCH,
            ResourceId.getDefaultInstance(),
            ResourceId.getDefaultInstance(),
            "t",
            null, // format (not relevant for this test)
            null, // columnIdAlgorithm (not relevant for this test)
            "{}", // schemaJson
            Map.of(), // properties
            List.of(), // partitionKeys
            Optional.empty(), // currentSnapshot
            Optional.empty(), // previousSnapshot
            Optional.empty(), // resolvedSnapshots
            Optional.empty(), // statsSummary
            List.of(), // dependentViews
            Map.of(), // engineHints
            Map.of()); // columnHints

    TestCatalogOverlay view = new TestCatalogOverlay();
    view.addNode(node);

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view,
            NameRef.getDefaultInstance(),
            ResourceId.getDefaultInstance(),
            EngineContext.empty());

    assertThat(ctx.resolve(ResourceId.getDefaultInstance())).isSameAs(node);
  }

  @Test
  void resolve_propagatesExceptionFromView() {
    RuntimeException ex = new IllegalStateException("boom");

    CatalogOverlay view =
        new TestCatalogOverlay() {
          @Override
          public Optional<GraphNode> resolve(ResourceId id) {
            throw ex;
          }
        };

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view,
            NameRef.getDefaultInstance(),
            ResourceId.getDefaultInstance(),
            EngineContext.empty());

    assertThatThrownBy(() -> ctx.resolve(ResourceId.getDefaultInstance()))
        .isInstanceOf(ex.getClass())
        .hasMessageContaining("boom");
  }

  @Test
  void listTables_delegates() {
    UserTableNode t =
        new UserTableNode(
            ResourceId.getDefaultInstance(),
            0L,
            Instant.EPOCH,
            ResourceId.getDefaultInstance(),
            ResourceId.getDefaultInstance(),
            "t",
            null,
            null,
            "{}",
            Map.of(),
            List.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            List.of(),
            Map.of(),
            Map.of());

    TestCatalogOverlay view = new TestCatalogOverlay();
    view.addRelation(ResourceId.getDefaultInstance(), t);

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view,
            NameRef.getDefaultInstance(),
            ResourceId.getDefaultInstance(),
            EngineContext.empty());

    assertThat(ctx.listTables(ResourceId.getDefaultInstance())).containsExactly(t);
  }

  @Test
  void listViews_delegates() {
    ViewNode v =
        new ViewNode(
            ResourceId.getDefaultInstance(),
            0L,
            Instant.EPOCH,
            ResourceId.getDefaultInstance(),
            ResourceId.getDefaultInstance(),
            "v",
            "SELECT 1", // sql
            "sql", // dialect
            List.of(), // outputColumns
            List.of(), // baseRelations
            List.of(), // creationSearchPath
            GraphNodeOrigin.USER, // origin
            Map.of(), // properties
            Optional.empty(), // owner
            Map.of(), // columnHints
            Map.of()); // engineHints

    TestCatalogOverlay view = new TestCatalogOverlay();
    view.addRelation(ResourceId.getDefaultInstance(), v);

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view,
            NameRef.getDefaultInstance(),
            ResourceId.getDefaultInstance(),
            EngineContext.empty());

    assertThat(ctx.listViews(ResourceId.getDefaultInstance())).containsExactly(v);
  }

  @Test
  void listNamespaces_delegates() {
    NamespaceNode n =
        new NamespaceNode(
            ResourceId.getDefaultInstance(),
            0L,
            Instant.EPOCH,
            ResourceId.getDefaultInstance(),
            List.of("public"), // pathSegments
            "n",
            GraphNodeOrigin.USER,
            Map.of(), // properties
            Map.of()); // engineHints

    TestCatalogOverlay view = new TestCatalogOverlay();
    view.addNode(n);

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            view,
            NameRef.getDefaultInstance(),
            ResourceId.getDefaultInstance(),
            EngineContext.empty());

    assertThat(ctx.listNamespaces()).containsExactly(n);
  }

  @Test
  void contextFields_areRetained_basic() {
    NameRef name = NameRef.newBuilder().addPath("a").setName("b").build();
    ResourceId catalog = ResourceId.newBuilder().setId("cat").build();

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(new TestCatalogOverlay(), name, catalog, EngineContext.empty());

    assertThat(ctx.name()).isEqualTo(name);
    assertThat(ctx.queryDefaultCatalogId()).isEqualTo(catalog);
  }
}

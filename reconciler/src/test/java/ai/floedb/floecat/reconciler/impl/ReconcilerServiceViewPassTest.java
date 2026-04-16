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

package ai.floedb.floecat.reconciler.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import java.util.List;
import org.junit.jupiter.api.Test;

class ReconcilerServiceViewPassTest extends AbstractReconcilerServiceTestBase {

  @Test
  void viewPassSyncsViewsViaEnsureView() {
    var viewDesc =
        new ai.floedb.floecat.connector.spi.FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "revenue_view",
            "SELECT amount FROM sales",
            "spark",
            List.of("src_ns"),
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"amount\",\"type\":\"double\",\"nullable\":true}]}");

    var capturingBackend = new ViewCapturingBackend(activeConnector());
    service.backend = capturingBackend;
    service.connectorOpener = cfg -> new FakeConnector(List.of(viewDesc));

    var result = service.reconcile(principal, connectorId, true, null);

    assertThat(result.ok()).isTrue();
    assertThat(result.scanned).isEqualTo(1);
    assertThat(result.changed).isEqualTo(1);
    assertThat(capturingBackend.capturedViews).hasSize(1);
    ViewSpec spec = capturingBackend.capturedViews.get(0);
    assertThat(spec.getDisplayName()).isEqualTo("revenue_view");
    assertThat(spec.getSqlDefinitionsList()).hasSize(1);
    assertThat(spec.getSqlDefinitions(0).getSql()).isEqualTo("SELECT amount FROM sales");
    assertThat(spec.getSqlDefinitions(0).getDialect()).isEqualTo("spark");
    assertThat(spec.getOutputColumnsCount()).isEqualTo(1);
    assertThat(spec.getOutputColumns(0).getName()).isEqualTo("amount");
    assertThat(spec.getOutputColumns(0).getLogicalType()).isEqualTo("DOUBLE");
    assertThat(spec.getCreationSearchPathList()).containsExactly("src_ns");
    assertThat(capturingBackend.capturedIdempotencyKeys).containsExactly("dest_ns.revenue_view");
  }

  @Test
  void viewPassCountsErrorWhenListViewDescriptorsThrows() {
    var capturingBackend = new ViewCapturingBackend(activeConnector());
    service.backend = capturingBackend;
    service.connectorOpener =
        cfg ->
            new FakeConnector(List.of()) {
              @Override
              public List<ai.floedb.floecat.connector.spi.FloecatConnector.ViewDescriptor>
                  listViewDescriptors(String ns) {
                throw new RuntimeException("UC unavailable");
              }
            };

    var result = service.reconcile(principal, connectorId, true, null);

    assertThat(result.errors).isGreaterThanOrEqualTo(1);
    assertThat(capturingBackend.capturedViews).isEmpty();
    assertThat(result.error).isNotNull();
    assertThat(result.error.getMessage()).contains("listViewDescriptors");
  }

  @Test
  void viewPassSkipsViewWithBlankSql() {
    var noSql =
        new ai.floedb.floecat.connector.spi.FloecatConnector.ViewDescriptor(
            "src_cat.src_ns", "empty_view", "", "spark", List.of("src_cat", "src_ns"), "");

    var capturingBackend = new ViewCapturingBackend(activeConnector());
    service.backend = capturingBackend;
    service.connectorOpener = cfg -> new FakeConnector(List.of(noSql));

    var result = service.reconcile(principal, connectorId, true, null);

    assertThat(result.ok()).isTrue();
    assertThat(result.scanned).isEqualTo(0);
    assertThat(result.changed).isEqualTo(0);
    assertThat(capturingBackend.capturedViews).isEmpty();
  }

  @Test
  void viewPassSkipsViewWithNoOutputColumns() {
    var noSchema =
        new ai.floedb.floecat.connector.spi.FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "no_cols_view",
            "SELECT 1",
            "spark",
            List.of(),
            "{\"type\":\"struct\",\"fields\":[]}");

    var capturingBackend = new ViewCapturingBackend(activeConnector());
    service.backend = capturingBackend;
    service.connectorOpener = cfg -> new FakeConnector(List.of(noSchema));

    var result = service.reconcile(principal, connectorId, true, null);

    assertThat(result.ok()).isTrue();
    assertThat(result.scanned).isEqualTo(0);
    assertThat(result.changed).isEqualTo(0);
    assertThat(capturingBackend.capturedViews).isEmpty();
  }

  @Test
  void viewPassCountsErrorWhenEnsureViewThrows() {
    var view1 =
        new ai.floedb.floecat.connector.spi.FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "bad_view",
            "SELECT a FROM t",
            "spark",
            List.of("src_cat", "src_ns"),
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"int\",\"nullable\":true}]}");
    var view2 =
        new ai.floedb.floecat.connector.spi.FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "good_view",
            "SELECT b FROM t",
            "spark",
            List.of("src_cat", "src_ns"),
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"b\",\"type\":\"double\",\"nullable\":true}]}");

    var capturingBackend =
        new ViewCapturingBackend(activeConnector()) {
          @Override
          public ResourceId ensureView(ReconcileContext ctx, ViewSpec spec, String idempotencyKey) {
            if ("dest_ns.bad_view".equals(idempotencyKey)) {
              throw new RuntimeException("backend error for " + spec.getDisplayName());
            }
            return super.ensureView(ctx, spec, idempotencyKey);
          }
        };
    service.backend = capturingBackend;
    service.connectorOpener = cfg -> new FakeConnector(List.of(view1, view2));

    var result = service.reconcile(principal, connectorId, true, null);

    assertThat(result.errors).isEqualTo(1);
    assertThat(result.changed).isEqualTo(1);
    assertThat(result.error).isNotNull();
    assertThat(result.error.getMessage()).contains("bad_view");
    assertThat(capturingBackend.capturedViews).hasSize(1);
    assertThat(capturingBackend.capturedViews.get(0).getDisplayName()).isEqualTo("good_view");
  }

  @Test
  void viewPassDoesNotCountAlreadyExistingView() {
    var viewDesc =
        new ai.floedb.floecat.connector.spi.FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "existing_view",
            "SELECT x FROM t",
            "spark",
            List.of("src_cat", "src_ns"),
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"x\",\"type\":\"int\",\"nullable\":false}]}");

    var capturingBackend =
        new ViewCapturingBackend(activeConnector()) {
          @Override
          public ResourceId ensureView(ReconcileContext ctx, ViewSpec spec, String idempotencyKey) {
            capturedViews.add(spec);
            capturedIdempotencyKeys.add(idempotencyKey);
            return ResourceId.getDefaultInstance();
          }
        };
    service.backend = capturingBackend;
    service.connectorOpener = cfg -> new FakeConnector(List.of(viewDesc));

    var result = service.reconcile(principal, connectorId, true, null);

    assertThat(result.ok()).isTrue();
    assertThat(result.scanned).isEqualTo(1);
    assertThat(result.changed).isEqualTo(0);
    assertThat(capturingBackend.capturedViews).hasSize(1);
  }

  @Test
  void viewPassCountsUpdatedExistingViewAsChanged() {
    var viewDesc =
        new ai.floedb.floecat.connector.spi.FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "existing_view",
            "SELECT x FROM t",
            "spark",
            List.of("src_ns"),
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"x\",\"type\":\"int\",\"nullable\":false}]}");

    var capturingBackend =
        new ViewCapturingBackend(activeConnector()) {
          @Override
          public ResourceId ensureView(ReconcileContext ctx, ViewSpec spec, String idempotencyKey) {
            capturedViews.add(spec);
            capturedIdempotencyKeys.add(idempotencyKey);
            return ResourceId.newBuilder().setId("updated-view").build();
          }
        };
    service.backend = capturingBackend;
    service.connectorOpener = cfg -> new FakeConnector(List.of(viewDesc));

    var result = service.reconcile(principal, connectorId, true, null);

    assertThat(result.ok()).isTrue();
    assertThat(result.scanned).isEqualTo(1);
    assertThat(result.changed).isEqualTo(1);
    assertThat(capturingBackend.capturedViews).hasSize(1);
  }

  @Test
  void viewsOnlyNamespaceWithTableFilterSyncsViews() {
    var viewDesc =
        new ai.floedb.floecat.connector.spi.FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "revenue_view",
            "SELECT amount FROM sales",
            "spark",
            List.of("src_ns"),
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"amount\",\"type\":\"double\",\"nullable\":true}]}");

    var capturingBackend = new ViewCapturingBackend(activeConnector());
    service.backend = capturingBackend;
    service.connectorOpener = cfg -> new FakeConnector(List.of(viewDesc));

    ReconcileScope scope = ReconcileScope.of(List.of(List.of("dest_ns")), "some_table", List.of());
    var result = service.reconcile(principal, connectorId, true, scope);

    assertThat(result.errors).isEqualTo(1);
    assertThat(result.error).isNotNull();
    assertThat(result.error.getMessage()).contains("No tables matched scope");
    assertThat(capturingBackend.capturedViews).hasSize(1);
    assertThat(capturingBackend.capturedViews.get(0).getDisplayName()).isEqualTo("revenue_view");
  }

  @Test
  void reconcileViewsOnlySkipsTableWorkAndStillSyncsViews() {
    var viewDesc =
        new ai.floedb.floecat.connector.spi.FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "revenue_view",
            "SELECT amount FROM sales",
            "spark",
            List.of("src_ns"),
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"amount\",\"type\":\"double\",\"nullable\":true}]}");

    var capturingBackend = new ViewCapturingBackend(activeConnector());
    service.backend = capturingBackend;
    service.connectorOpener =
        cfg ->
            new FakeConnector(List.of(viewDesc)) {
              @Override
              public List<String> listTables(String namespaceFq) {
                throw new AssertionError("views-only reconcile should not enumerate tables");
              }

              @Override
              public TableDescriptor describe(String namespaceFq, String tableName) {
                throw new AssertionError("views-only reconcile should not describe tables");
              }
            };

    ReconcileScope scope = ReconcileScope.of(List.of(List.of("dest_ns")), "some_table", List.of());
    var result =
        service.reconcileViewsOnly(
            principal,
            connectorId,
            scope,
            null,
            () -> false,
            (ts, tc, vs, vc, e, sp, stp, m) -> {});

    assertThat(result.ok()).isTrue();
    assertThat(result.errors).isEqualTo(0);
    assertThat(result.scanned).isEqualTo(1);
    assertThat(result.changed).isEqualTo(1);
    assertThat(capturingBackend.capturedViews).hasSize(1);
    assertThat(capturingBackend.capturedViews.get(0).getDisplayName()).isEqualTo("revenue_view");
  }

  @Test
  void reconcileViewExecutesSinglePlannedViewTask() {
    var viewDesc =
        new ai.floedb.floecat.connector.spi.FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "revenue_view",
            "SELECT amount FROM sales",
            "spark",
            List.of("src_ns"),
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"amount\",\"type\":\"double\",\"nullable\":true}]}");

    var capturingBackend = new ViewCapturingBackend(activeConnector());
    service.backend = capturingBackend;
    service.connectorOpener = cfg -> new FakeConnector(List.of(viewDesc));

    var result =
        service.reconcileView(
            principal,
            connectorId,
            ReconcileScope.empty(),
            ReconcileViewTask.of("src_cat.src_ns", "revenue_view", "dest_ns", "revenue_view"),
            null,
            () -> false,
            (ts, tc, vs, vc, e, sp, stp, m) -> {});

    assertThat(result.ok()).isTrue();
    assertThat(result.scanned).isEqualTo(1);
    assertThat(result.changed).isEqualTo(1);
    assertThat(capturingBackend.capturedViews).hasSize(1);
    assertThat(capturingBackend.capturedViews.get(0).getDisplayName()).isEqualTo("revenue_view");
  }
}

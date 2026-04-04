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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Verifies the default {@link FloecatConnector#listViewDescriptors} chain: a connector that only
 * overrides {@link FloecatConnector#listViews} and {@link FloecatConnector#describeView} (the N+1
 * fallback path) should have its results composed correctly by the default implementation.
 */
class FloecatConnectorDefaultChainTest {

  /**
   * Minimal stub that returns views via the default chain ({@code listViews} + {@code
   * describeView}), intentionally NOT overriding {@code listViewDescriptors}.
   */
  private static class StubConnector implements FloecatConnector {
    @Override
    public String id() {
      return "stub";
    }

    @Override
    public ConnectorFormat format() {
      return ConnectorFormat.CF_UNKNOWN;
    }

    @Override
    public List<String> listNamespaces() {
      return List.of();
    }

    @Override
    public List<String> listTables(String namespaceFq) {
      return List.of();
    }

    @Override
    public List<PlannedTableTask> planTableTasks(TablePlanningRequest request) {
      return List.of();
    }

    @Override
    public TableDescriptor describe(String namespaceFq, String tableName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<SnapshotBundle> enumerateSnapshotsWithStats(
        String namespaceFq,
        String tableName,
        ResourceId destinationTableId,
        Set<String> includeColumns) {
      return List.of();
    }

    @Override
    public void close() {}

    // Overrides only listViews() + describeView(); listViewDescriptors() uses the default chain.

    @Override
    public List<String> listViews(String namespaceFq) {
      return List.of("view_a", "view_b", "view_c");
    }

    @Override
    public Optional<ViewDescriptor> describeView(String namespaceFq, String name) {
      if ("view_b".equals(name)) {
        // Simulates a view that disappeared between list and describe (e.g. concurrent drop).
        return Optional.empty();
      }
      return Optional.of(
          new ViewDescriptor(namespaceFq, name, "SELECT 1", "spark", List.of(namespaceFq), "{}"));
    }
  }

  @Test
  void defaultChainFiltersOutEmptyDescribeViewResults() {
    FloecatConnector connector = new StubConnector();

    List<FloecatConnector.ViewDescriptor> descriptors = connector.listViewDescriptors("cat.schema");

    // view_b returned Optional.empty() and must be silently skipped.
    assertThat(descriptors).hasSize(2);
    assertThat(descriptors.stream().map(FloecatConnector.ViewDescriptor::name))
        .containsExactly("view_a", "view_c");
    // Verify fields round-trip correctly.
    assertThat(descriptors.get(0).dialect()).isEqualTo("spark");
    assertThat(descriptors.get(0).namespaceFq()).isEqualTo("cat.schema");
  }

  @Test
  void defaultChainIsFailFastWhenDescribeViewThrows() {
    // The default listViewDescriptors() does not swallow exceptions — the first throwing
    // describeView call propagates immediately, skipping any remaining views.
    FloecatConnector connector =
        new StubConnector() {
          @Override
          public Optional<FloecatConnector.ViewDescriptor> describeView(
              String namespaceFq, String name) {
            if ("view_b".equals(name)) {
              throw new RuntimeException("catalog unavailable");
            }
            return super.describeView(namespaceFq, name);
          }
        };

    assertThatThrownBy(() -> connector.listViewDescriptors("cat.schema"))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("catalog unavailable");
  }
}

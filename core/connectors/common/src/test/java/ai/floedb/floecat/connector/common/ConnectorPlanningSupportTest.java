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

package ai.floedb.floecat.connector.common;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConnectorPlanningSupportTest {

  @Test
  void planTableTasksReturnsNoTasksForNamespaceScopeMiss() {
    var request =
        new FloecatConnector.TablePlanningRequest(
            "src.ns", "", "dest.other_ns", "", List.of(List.of("dest", "requested_ns")), "");

    var planned = ConnectorPlanningSupport.planTableTasks(request, ignored -> List.of("orders"));

    assertThat(planned).isEmpty();
  }

  @Test
  void planViewTasksReturnsTasksForNamespaceScopeHit() {
    var request =
        new FloecatConnector.ViewPlanningRequest(
            "src.ns", "dest.requested_ns", List.of(List.of("dest", "requested_ns")));

    var planned =
        ConnectorPlanningSupport.planViewTasks(
            request,
            ignored ->
                List.of(
                    new FloecatConnector.ViewDescriptor(
                        "src.ns", "orders_view", "SELECT 1", "spark", List.of(), "{}")));

    assertThat(planned)
        .containsExactly(
            new FloecatConnector.PlannedViewTask(
                "src.ns", "orders_view", "dest.requested_ns", "orders_view"));
  }
}

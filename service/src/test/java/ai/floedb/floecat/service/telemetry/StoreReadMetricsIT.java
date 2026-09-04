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

package ai.floedb.floecat.service.telemetry;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Proves deployed store decorators feed the production Prometheus exporter. */
@QuarkusTest
class StoreReadMetricsIT {
  @Inject PointerStore pointers;

  @TestHTTPResource(value = "/metrics", management = true)
  URI metricsEndpoint;

  @Test
  void pointerReadIsAvailableAtTheProductionMetricsEndpoint() {
    pointers.get(Keys.accountPointerByName("telemetry-sensitive-probe"));

    String metrics =
        given().when().get(metricsEndpoint).then().statusCode(200).extract().asString();
    List<String> storeLines =
        metrics.lines().filter(line -> line.startsWith("floecat_core_store_")).toList();

    assertThat(storeLines)
        .anySatisfy(
            line ->
                assertThat(line)
                    .startsWith("floecat_core_store_requests_total")
                    .contains("component=\"pointer_store\"")
                    .contains("operation=\"get\"")
                    .contains("result=\"success\""))
        .anySatisfy(
            line ->
                assertThat(line)
                    .startsWith("floecat_core_store_items_total")
                    .contains("component=\"pointer_store\"")
                    .contains("operation=\"get\"")
                    .contains("result=\"success\""))
        .allSatisfy(line -> assertThat(line).doesNotContain("sensitive-probe"));
  }
}

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

package ai.floedb.floecat.service.account;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;

/** Exercises the HTTP contract used by the pod lifecycle hook and the management controller. */
@QuarkusTest
@TestProfile(FloecatDrainEndpointTestProfile.class)
class FloecatDrainEndpointIT {
  @Inject AccountGcAuthority authority;

  @TestHTTPResource("/internal/drain")
  URI drainUri;

  @Test
  void lifecycleHookStartsDrainAndWaitsForExistingWork() throws Exception {
    var resolution = authority.admitResolution("drain-http-test");
    try {
      HttpClient client = HttpClient.newHttpClient();

      HttpResponse<String> start =
          client.send(request("?wait=false"), HttpResponse.BodyHandlers.ofString());
      assertThat(start.statusCode()).isEqualTo(202);
      assertThat(start.body()).contains("\"draining\":true", "\"drained\":false");

      HttpResponse<String> repeatedStart =
          client.send(request("?wait=false"), HttpResponse.BodyHandlers.ofString());
      assertThat(repeatedStart.statusCode()).isEqualTo(202);

      resolution.close();
      HttpResponse<String> drained =
          client.send(request("?wait=true&timeoutMs=1000"), HttpResponse.BodyHandlers.ofString());
      assertThat(drained.statusCode()).isEqualTo(200);
      assertThat(drained.body()).contains("\"drained\":true");
    } finally {
      resolution.close();
    }
  }

  private HttpRequest request(String query) {
    return HttpRequest.newBuilder(URI.create(drainUri + query)).GET().build();
  }
}

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

/** The HTTP contract the pod lifecycle hook relies on, exercised over the loopback (pod-local). */
@QuarkusTest
@TestProfile(FloecatDrainEndpointTestProfile.class)
class FloecatDrainEndpointIT {
  @Inject AccountAssignment assignment;

  @TestHTTPResource("/internal/drain")
  URI drainUri;

  @Test
  void lifecycleHookDrainsAndWaitsForInFlightWork() throws Exception {
    String accountId = "drain-http-account";
    assignment.admitResolution(accountId).close();
    HttpClient client = HttpClient.newHttpClient();

    HttpResponse<String> before = send(client, "", "GET");
    assertThat(before.statusCode()).isEqualTo(200);
    assertThat(before.body()).contains("\"draining\":false", "\"servingAccounts\":1");

    assertThat(send(client, "?wait=true&timeoutMs=-1", "GET").statusCode()).isEqualTo(400);
    assertThat(assignment.status().processDraining()).isFalse();

    var resolution = assignment.admitResolution(accountId);
    try {
      HttpResponse<String> begun = send(client, "", "POST");
      assertThat(begun.statusCode()).isEqualTo(202);
      assertThat(begun.body()).contains("\"draining\":true", "\"drained\":false");

      HttpResponse<String> waited = send(client, "?wait=true&timeoutMs=100", "GET");
      assertThat(waited.statusCode()).isEqualTo(202);
    } finally {
      resolution.close();
    }

    HttpResponse<String> drained = send(client, "?wait=true&timeoutMs=1000", "GET");
    assertThat(drained.statusCode()).isEqualTo(200);
    assertThat(drained.body()).contains("\"drained\":true", "\"activeResolutions\":0");
  }

  private HttpResponse<String> send(HttpClient client, String query, String method)
      throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(drainUri + query))
            .method(method, HttpRequest.BodyPublishers.noBody())
            .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }
}

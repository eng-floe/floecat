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

import ai.floedb.floecat.connector.spi.AuthProvider;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Map;

public final class UcHttp {
  private final String host;
  private final AuthProvider auth;
  private final Duration readTimeout;
  private final HttpClient client;

  public UcHttp(String host, int connectMs, int readMs, AuthProvider auth) {
    this.host = host;
    this.auth = auth;
    this.readTimeout = Duration.ofMillis(readMs);
    this.client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(connectMs)).build();
  }

  public HttpResponse<String> get(String pathAndQuery) throws IOException, InterruptedException {
    return send(request(pathAndQuery).GET().build());
  }

  public HttpResponse<String> post(String pathAndQuery, String jsonBody)
      throws IOException, InterruptedException {
    return send(
        request(pathAndQuery)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
            .build());
  }

  private HttpRequest.Builder request(String pathAndQuery) {
    var request =
        HttpRequest.newBuilder().uri(URI.create(host + pathAndQuery)).timeout(readTimeout);
    auth.applyHeaders(Map.of()).forEach(request::header);
    return request;
  }

  private HttpResponse<String> send(HttpRequest request) throws IOException, InterruptedException {
    return client.send(request, BodyHandlers.ofString());
  }
}

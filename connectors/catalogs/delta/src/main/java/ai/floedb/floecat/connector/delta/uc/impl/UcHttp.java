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
  private final Duration readMs;
  private final Duration connectMs;
  private final HttpClient client;

  public UcHttp(String host, int connectMs, int readMs, AuthProvider auth) {
    this.host = host;
    this.auth = auth;
    this.readMs = Duration.ofMillis(readMs);
    this.connectMs = Duration.ofMillis(connectMs);
    this.client = HttpClient.newBuilder().connectTimeout(this.connectMs).build();
  }

  public HttpResponse<String> get(String pathAndQuery) throws Exception {
    var req = HttpRequest.newBuilder().uri(URI.create(host + pathAndQuery)).timeout(readMs);
    var headers = auth.applyHeaders(Map.of());
    headers.forEach(req::header);

    return client.send(req.GET().build(), BodyHandlers.ofString());
  }
}

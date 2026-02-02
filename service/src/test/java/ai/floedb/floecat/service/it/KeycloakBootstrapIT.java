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

package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.account.rpc.AccountServiceGrpc;
import ai.floedb.floecat.account.rpc.AccountSpec;
import ai.floedb.floecat.account.rpc.CreateAccountRequest;
import ai.floedb.floecat.account.rpc.ListAccountsRequest;
import ai.floedb.floecat.service.it.profiles.KeycloakOidcProfile;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(KeycloakOidcProfile.class)
class KeycloakBootstrapIT {

  private static final Metadata.Key<String> AUTH_HEADER =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

  @GrpcClient("floecat")
  AccountServiceGrpc.AccountServiceBlockingStub accounts;

  @Test
  void bootstrapAdminAccountAndCreateTenant() throws Exception {
    String token = fetchAccessToken();
    Metadata metadata = new Metadata();
    metadata.put(AUTH_HEADER, "Bearer " + token);

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    List<Account> existing =
        stub.listAccounts(ListAccountsRequest.getDefaultInstance()).getAccountsList();

    assertFalse(existing.isEmpty());
    assertTrue(
        existing.stream().anyMatch(account -> "admin".equals(account.getDisplayName())),
        "admin account should be bootstrapped");

    String tenantName = "tenant-" + UUID.randomUUID();
    var created =
        stub.createAccount(
            CreateAccountRequest.newBuilder()
                .setSpec(AccountSpec.newBuilder().setDisplayName(tenantName).build())
                .build());
    assertNotNull(created.getAccount().getResourceId().getId());
    assertTrue(created.getAccount().getDisplayName().equals(tenantName));
  }

  private static String fetchAccessToken() throws IOException, InterruptedException {
    String body =
        "client_id=floecat-client&client_secret=floecat-secret&grant_type=client_credentials";
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:12221/realms/floecat/protocol/openid-connect/token"))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

    HttpResponse<String> response =
        HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() / 100 != 2) {
      throw new IllegalStateException(
          "Keycloak token request failed: " + response.statusCode() + " " + response.body());
    }
    return extractJsonValue(response.body(), "access_token");
  }

  private static String extractJsonValue(String json, String key) {
    String needle = "\"" + key + "\":\"";
    int start = json.indexOf(needle);
    if (start < 0) {
      throw new IllegalStateException("Missing key " + key + " in response: " + json);
    }
    start += needle.length();
    int end = json.indexOf('"', start);
    if (end < 0) {
      throw new IllegalStateException("Malformed JSON response: " + json);
    }
    return json.substring(start, end);
  }
}

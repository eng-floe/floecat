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

package ai.floedb.floecat.gateway.iceberg.minimal.resources;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ListTablesResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcClients;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.minimal.util.TestKeyPair;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.jwt.build.Jwt;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.Mockito;

@QuarkusTest
@TestProfile(OidcGatewayProfile.class)
@EnabledIfSystemProperty(named = "floecat.test.oidc", matches = "true")
class OidcAccountClaimTest {
  @InjectMock GrpcWithHeaders grpc;
  @InjectMock GrpcClients clients;

  private DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub;
  private TableServiceGrpc.TableServiceBlockingStub tableStub;

  @BeforeEach
  void setUp() {
    directoryStub = Mockito.mock(DirectoryServiceGrpc.DirectoryServiceBlockingStub.class);
    tableStub = Mockito.mock(TableServiceGrpc.TableServiceBlockingStub.class);

    when(clients.directory()).thenReturn(directoryStub);
    when(clients.table()).thenReturn(tableStub);
    when(grpc.raw()).thenReturn(clients);
    when(grpc.withHeaders(directoryStub)).thenReturn(directoryStub);
    when(grpc.withHeaders(tableStub)).thenReturn(tableStub);
  }

  @Test
  void derivesAccountIdFromJwtClaim() throws Exception {
    ResourceId nsId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build());
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .build();
    when(tableStub.listTables(any()))
        .thenReturn(
            ListTablesResponse.newBuilder()
                .addTables(table)
                .setPage(PageResponse.newBuilder().build())
                .build());

    given()
        .header("authorization", "Bearer " + jwtToken("5eaa9cd5-7d08-3750-9457-cfe800b0b9d2"))
        .when()
        .get("/v1/foo/namespaces/db/tables")
        .then()
        .statusCode(200)
        .body("identifiers[0].name", equalTo("orders"));
  }

  private static String jwtToken(String accountId) throws Exception {
    Instant now = Instant.now();
    return Jwt.claims()
        .issuer("https://floecat.test")
        .audience("floecat-client")
        .subject("it-user")
        .claim("account_id", accountId)
        .issuedAt(now)
        .expiresAt(now.plusSeconds(3600))
        .sign(TestKeyPair.privateKey());
  }
}

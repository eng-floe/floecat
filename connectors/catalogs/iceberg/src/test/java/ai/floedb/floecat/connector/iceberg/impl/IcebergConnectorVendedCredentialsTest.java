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

package ai.floedb.floecat.connector.iceberg.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Test;

/**
 * Covers {@code vendStorageCredentials}, which copies credentials out of a delegated {@code
 * loadTable}.
 *
 * <p>The property maps here are the real shape observed from Polaris on 2026-08-08 -- notably that
 * a delegated load returns the catalog's own {@code token} alongside the storage credentials, which
 * is why the copy is an allowlist.
 */
class IcebergConnectorVendedCredentialsTest {

  private static IcebergConnector connectorReturning(Map<String, String> ioProperties) {
    FileIO io =
        ioProperties == null
            ? null
            : (FileIO)
                Proxy.newProxyInstance(
                    IcebergConnectorVendedCredentialsTest.class.getClassLoader(),
                    new Class<?>[] {FileIO.class},
                    (proxy, method, args) ->
                        switch (method.getName()) {
                          case "properties" -> ioProperties;
                          case "toString" -> "fake-io";
                          case "hashCode" -> System.identityHashCode(proxy);
                          case "equals" -> proxy == args[0];
                          default -> throw new UnsupportedOperationException(method.getName());
                        });

    Table table =
        (Table)
            Proxy.newProxyInstance(
                IcebergConnectorVendedCredentialsTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "io" -> io;
                      case "name" -> "fake-table";
                      case "toString" -> "fake-table";
                      case "hashCode" -> System.identityHashCode(proxy);
                      case "equals" -> proxy == args[0];
                      default -> throw new UnsupportedOperationException(method.getName());
                    });

    return new IcebergConnector("test", null, null, null, false, 0.0d, 0L, null) {
      @Override
      public List<String> listNamespaces() {
        return List.of();
      }

      @Override
      public List<String> listTables(String namespaceFq) {
        return List.of();
      }

      @Override
      protected Table loadTableFromSource(String namespaceFq, String tableName) {
        return table;
      }
    };
  }

  /** The property shape Polaris actually returns for a delegated load. */
  private static Map<String, String> polarisDelegatedProperties() {
    return Map.of(
        "s3.access-key-id", "ASIAVENDED",
        "s3.secret-access-key", "vended-secret",
        "s3.session-token", "vended-session",
        "s3.session-token-expires-at-ms", "1786000000000",
        "expiration-time", "whatever",
        "token", "CATALOG-OAUTH-TOKEN",
        "uri", "https://polaris.example/api/catalog");
  }

  @Test
  void copiesVendedStorageCredentialsAndExpiry() {
    Optional<FloecatConnector.VendedStorageCredentials> vended =
        connectorReturning(polarisDelegatedProperties())
            .vendStorageCredentials("tpch_10", "customer");

    assertTrue(vended.isPresent());
    assertEquals("ASIAVENDED", vended.get().properties().get("s3.access-key-id"));
    assertEquals("vended-secret", vended.get().properties().get("s3.secret-access-key"));
    assertEquals("vended-session", vended.get().properties().get("s3.session-token"));
    assertEquals(Instant.ofEpochMilli(1786000000000L), vended.get().expiresAt());
  }

  /**
   * The catalog's own bearer token shares the property map with the storage credentials. Copying it
   * would hand a catalog credential to whatever reads data files, so the copy is an allowlist and
   * this test is the thing that keeps it one.
   */
  @Test
  void neverCopiesTheCatalogAuthToken() {
    Optional<FloecatConnector.VendedStorageCredentials> vended =
        connectorReturning(polarisDelegatedProperties())
            .vendStorageCredentials("tpch_10", "customer");

    assertTrue(vended.isPresent());
    Map<String, String> props = vended.get().properties();
    assertFalse(
        props.containsKey("token"), "catalog auth token must not reach storage credentials");
    assertFalse(props.containsValue("CATALOG-OAUTH-TOKEN"));
    assertFalse(props.containsKey("uri"));
    assertEquals(3, props.size(), "only the three s3 credential keys should be copied");
  }

  /** A catalog that does not delegate returns a fine table with no credentials on its FileIO. */
  @Test
  void nonDelegatingCatalogYieldsEmptyRatherThanFailing() {
    Map<String, String> noCredentials =
        Map.of("uri", "https://s3tables.example/iceberg", "s3.region", "us-east-1");

    assertTrue(
        connectorReturning(noCredentials).vendStorageCredentials("tpch_10", "customer").isEmpty());
  }

  @Test
  void missingFileIoYieldsEmpty() {
    assertTrue(connectorReturning(null).vendStorageCredentials("tpch_10", "customer").isEmpty());
  }

  /**
   * Absent expiry stays null rather than being invented -- guessing a TTL here would produce
   * credentials that expire mid-read with no way to tell.
   *
   * <p>Null is not "do not cache", which an earlier version of this comment claimed. The reconcile
   * worker treats a missing expiry as "not refreshable" and embeds the credentials statically, so
   * the service refuses them outright; see {@code requireUsableExpiry}. This connector's job is to
   * report the absence faithfully, not to decide what it means.
   */
  @Test
  void absentExpiryIsNullNotGuessed() {
    Map<String, String> noExpiry =
        Map.of(
            "s3.access-key-id", "ASIAVENDED",
            "s3.secret-access-key", "vended-secret",
            "s3.session-token", "vended-session");

    Optional<FloecatConnector.VendedStorageCredentials> vended =
        connectorReturning(noExpiry).vendStorageCredentials("tpch_10", "customer");

    assertTrue(vended.isPresent());
    assertNull(vended.get().expiresAt());
  }

  @Test
  void unparseableExpiryIsIgnoredRatherThanThrowing() {
    Map<String, String> badExpiry =
        Map.of(
            "s3.access-key-id", "ASIAVENDED",
            "s3.secret-access-key", "vended-secret",
            "s3.session-token", "vended-session",
            "s3.session-token-expires-at-ms", "not-a-number");

    Optional<FloecatConnector.VendedStorageCredentials> vended =
        connectorReturning(badExpiry).vendStorageCredentials("tpch_10", "customer");

    assertTrue(vended.isPresent());
    assertNull(vended.get().expiresAt());
  }
}

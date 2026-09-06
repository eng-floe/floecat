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

package ai.floedb.floecat.catalog.access;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The two records of this name render a scope prefix by the same rules, in code neither can share:
 * this module carries no production dependencies and the connector SPI pulls in proto and types.
 *
 * <p>Each side has its own tests over its own copy, so a rule added to one and missed on the other
 * would leave a green suite. This is the test that does not: it drives both through {@code
 * toString} and compares what they print.
 */
class VendedStorageCredentialsParityTest {

  private static String connectorSide(String prefix) {
    return between(
        new ai.floedb.floecat.connector.spi.FloecatConnector.VendedStorageCredentials(
                Map.of("s3.access-key-id", "AKIAEXAMPLE"), prefix, Instant.EPOCH)
            .toString());
  }

  private static String catalogAccessSide(String prefix) {
    return between(
        new VendedStorageCredentials(
                Map.of("s3.access-key-id", "AKIAEXAMPLE"), prefix, Optional.of(Instant.EPOCH))
            .toString());
  }

  private static String between(String printed) {
    int from = printed.indexOf("scopePrefix=") + "scopePrefix=".length();
    return printed.substring(from, printed.indexOf(", expiresAt=", from));
  }

  @Test
  void bothRecordsRenderAScopePrefixIdentically() {
    for (String prefix :
        new String[] {
          "s3://bucket/orders",
          "s3://bucket/user@example.com/orders",
          "https://admin:hunter2@warehouse.example.com/orders",
          "https://tok@host/orders",
          "https://admin:pa?ss@host/orders",
          "https://admin:hunt#er2@host/orders",
          "abfss://warehouse@acct.dfs.core.windows.net/orders",
          "wasbs://warehouse@acct.blob.core.windows.net/orders",
          "abfss://admin:hunter2@acct.dfs.core.windows.net/orders",
          "https://acct.blob.core.windows.net/c/o?sv=2021&sig=SECRETSIG",
          "s3://bucket/orders#frag-SECRET",
          "s3://bucket/x\n2030-01-01 ERROR forged",
          "s3://bucket/x\u0085forged",
          "s3://bucket/x\u2028forged",
          "s3://bucket/x\u2029forged",
          "s3://bucket/orders ",
          "s3://bucket/orders\u0001",
          " s3://bucket/orders",
          "a".repeat(255) + "\uD834\uDD1E" + "a".repeat(100),
          "s3://bucket/" + "a".repeat(5000),
        }) {
      assertEquals(connectorSide(prefix), catalogAccessSide(prefix), prefix);
    }
  }
}

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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class VendedStorageCredentialsTest {

  private static VendedStorageCredentials withPrefix(String scopePrefix) {
    return new VendedStorageCredentials(
        Map.of("s3.access-key-id", "AKIAEXAMPLE", "s3.secret-access-key", "top-secret-key"),
        scopePrefix,
        Optional.of(Instant.parse("2030-01-01T00:00:00Z")));
  }

  @Test
  void keyNamesAreLoggedAndSecretsAreNot() {
    String printed = withPrefix("s3://bucket/orders").toString();

    assertFalse(printed.contains("top-secret-key"), printed);
    assertTrue(printed.contains("s3.secret-access-key"), printed);
    assertTrue(printed.contains("scopePrefix=s3://bucket/orders"), printed);
  }

  @Test
  void anAbsentScopePrefixIsDistinguishableFromOneThatWasSupplied() {
    // Callers spell absent as "" here, not null, so without a marker the common case prints an
    // empty slot -- losing the separation between "no prefix was supplied" and "one was and it is
    // wrong", which is the only reason this field is printed at all.
    assertTrue(
        withPrefix("").toString().contains("scopePrefix=<absent>"), withPrefix("").toString());
    assertTrue(
        withPrefix("   ").toString().contains("scopePrefix=<absent>"),
        withPrefix("   ").toString());
    assertTrue(
        withPrefix("s3://bucket/orders").toString().contains("scopePrefix=s3://bucket/orders"));
  }

  @Test
  void aPrefixIsPrintedWithoutThePartsOfAUriThatCanCarryASecret() {
    // Mirrors LogSafeText.location, which this record cannot call. The same four rules, but not
    // the same tests: this file exercises only this copy, and VendedStorageCredentialsParityTest
    // renders a fixed list of prefixes through both. That catches a rule changed on one side, not
    // a rule added to one side for a shape the list does not contain.
    assertFalse(
        withPrefix("https://acct.blob.core.windows.net/c/o?sv=2021&sig=SECRETSIG")
            .toString()
            .contains("SECRETSIG"));
    assertFalse(withPrefix("s3://bucket/orders#frag-SECRET").toString().contains("frag-SECRET"));
    assertFalse(
        withPrefix("https://admin:hunter2@warehouse.example.com/o").toString().contains("hunter2"));
    assertTrue(
        withPrefix("https://admin:hunter2@warehouse.example.com/o")
            .toString()
            .contains("warehouse.example.com/o"));
    // A ? or # inside the password must not cut the @ away with the tail.
    assertFalse(withPrefix("https://admin:pa?ss@host/o").toString().contains("admin:pa"));
    assertFalse(withPrefix("https://admin:hunt#er2@host/o").toString().contains("admin:hunt"));

    // ADLS and WASB put the container before the @, not a credential.
    assertTrue(
        withPrefix("abfss://warehouse@acct.dfs.core.windows.net/o")
            .toString()
            .contains("abfss://warehouse@acct.dfs.core.windows.net/o"));
    assertTrue(
        withPrefix("wasbs://warehouse@acct.blob.core.windows.net/o")
            .toString()
            .contains("wasbs://warehouse@acct.blob.core.windows.net/o"));
    // A colon settles it even on those schemes.
    assertFalse(
        withPrefix("abfss://admin:hunter2@acct.dfs.core.windows.net/o")
            .toString()
            .contains("hunter2"));

    // An @ in the path is part of a key, not a delimiter.
    assertTrue(
        withPrefix("s3://bucket/user@example.com/o")
            .toString()
            .contains("s3://bucket/user@example.com/o"));
  }

  @Test
  void truncationDoesNotCutASurrogatePairInHalf() {
    String printed = withPrefix("a".repeat(255) + "\uD834\uDD1E" + "a".repeat(100)).toString();

    for (int i = 0; i < printed.length(); i++) {
      if (Character.isHighSurrogate(printed.charAt(i))) {
        assertTrue(i + 1 < printed.length(), "lone high surrogate at " + i);
        assertTrue(Character.isLowSurrogate(printed.charAt(i + 1)), "unpaired at " + i);
      }
    }
  }

  @Test
  void aScopePrefixIsBoundedAndFlattenedBeforeItReachesALogLine() {
    // IcebergRestCatalogClient passes the upstream loadTable prefix straight through, so the value
    // has no length limit and no character restriction.
    String forged = withPrefix("s3://bucket/x\n2030-01-01 ERROR forged log line").toString();
    assertFalse(forged.contains("\n"), forged);
    assertTrue(forged.contains("s3://bucket/x?2030-01-01 ERROR"), forged);

    for (String terminator : new String[] {"\u0085", "\u2028", "\u2029"}) {
      String printed = withPrefix("s3://bucket/x" + terminator + "forged").toString();
      assertFalse(printed.contains(terminator), "U+" + (int) terminator.charAt(0) + ": " + printed);
      assertTrue(printed.contains("s3://bucket/x?forged"), printed);
    }

    String huge = withPrefix("s3://bucket/" + "a".repeat(5000)).toString();
    assertTrue(huge.length() < 500, "length " + huge.length());
    assertTrue(huge.contains("(5012 chars)"), huge);
  }
}

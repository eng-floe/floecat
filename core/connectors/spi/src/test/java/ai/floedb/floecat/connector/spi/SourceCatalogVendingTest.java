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

package ai.floedb.floecat.connector.spi;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Covers the two vend gates that live in this module. They ship here, beside every connector that
 * depends on them, so the rules are exercised by this module's own build rather than only when a
 * downstream connector module happens to be built.
 */
class SourceCatalogVendingTest {

  private static ConnectorConfig config(ConnectorConfig.Kind kind, Map<String, String> options) {
    return new ConnectorConfig(kind, "display", "https://example", options, null);
  }

  @Test
  void unityBackedDeltaConnectorOptsInWhenFlagIsTruthy() {
    for (String truthy : new String[] {"true", "1", "yes", "vended-credentials", "TRUE"}) {
      var cfg =
          config(
              ConnectorConfig.Kind.DELTA,
              Map.of("delta.source", "unity", DatabricksAccessDelegation.VEND_OPTION, truthy));
      assertThat(DatabricksAccessDelegation.declaresVendedCredentials(cfg)).as(truthy).isTrue();
    }
  }

  @Test
  void vendedCredentialsAreNotPrintedByToString() {
    var vended =
        new FloecatConnector.VendedStorageCredentials(
            Map.of(
                "s3.access-key-id", "AKIAEXAMPLE",
                "s3.secret-access-key", "top-secret-key",
                "s3.session-token", "top-secret-token"),
            "s3://bucket/prefix",
            Instant.parse("2030-01-01T00:00:00Z"));

    assertThat(vended.toString())
        .doesNotContain("top-secret-key", "top-secret-token", "AKIAEXAMPLE")
        .contains("s3.secret-access-key", "scopePrefix=s3://bucket/prefix", "2030-01-01T00:00:00Z");
  }

  @Test
  void anAbsentScopePrefixIsDistinguishableFromOneThatWasSupplied() {
    // The compact constructor folds blank to null, so a log line has to separate the two cases for
    // the printed prefix to answer anything about how a credential got scoped.
    for (String none : new String[] {null, "", "   "}) {
      var vended =
          new FloecatConnector.VendedStorageCredentials(
              Map.of("s3.access-key-id", "AKIAEXAMPLE"),
              none,
              Instant.parse("2030-01-01T00:00:00Z"));
      assertThat(vended.toString()).as("%s", none).contains("scopePrefix=<absent>");
    }
  }

  @Test
  void aScopePrefixIsBoundedAndFlattenedBeforeItReachesALogLine() {
    // The prefix comes from the catalog over the wire, with no length limit and no character
    // restriction, and toString feeds a log line.
    var forged =
        new FloecatConnector.VendedStorageCredentials(
            Map.of("s3.access-key-id", "AKIAEXAMPLE"),
            "s3://bucket/x\n2030-01-01 ERROR forged log line",
            Instant.parse("2030-01-01T00:00:00Z"));

    assertThat(forged.toString()).doesNotContain("\n").contains("s3://bucket/x?2030-01-01 ERROR");

    var huge =
        new FloecatConnector.VendedStorageCredentials(
            Map.of("s3.access-key-id", "AKIAEXAMPLE"),
            "s3://bucket/" + "a".repeat(5000),
            Instant.parse("2030-01-01T00:00:00Z"));

    assertThat(huge.toString()).hasSizeLessThan(500).contains("(5012 chars)");
  }

  @Test
  void aScopePrefixIsFlattenedForEveryCharacterALogRendererTreatsAsALineEnd() {
    // \p{Cntrl} is the POSIX ASCII class and matches neither U+0085 nor the Unicode separators
    // U+2028 and U+2029, all of which end a line for a Unicode-aware renderer.
    for (String terminator : new String[] {"\n", "\r", "\u0085", "\u2028", "\u2029"}) {
      var vended =
          new FloecatConnector.VendedStorageCredentials(
              Map.of("s3.access-key-id", "AKIAEXAMPLE"),
              "s3://bucket/x" + terminator + "2030-01-01 ERROR forged",
              Instant.parse("2030-01-01T00:00:00Z"));

      assertThat(vended.toString())
          .as("U+%04X", (int) terminator.charAt(0))
          .doesNotContain(terminator)
          .contains("s3://bucket/x?2030-01-01 ERROR");
    }
  }

  @Test
  void aPrefixIsPrintedWithoutThePartsOfAUriThatCanCarryASecret() {
    // A storage prefix is catalog-supplied and nothing makes it a bare s3://bucket/key. A query or
    // fragment on one is a presigned signature or a SAS token; userinfo is a password. The location
    // still has to be readable, which is the whole reason this field is not redacted outright.
    record Case(String prefix, String kept, String gone) {}
    for (Case c :
        new Case[] {
          new Case(
              "https://acct.blob.core.windows.net/c/orders?sv=2021&sig=SECRETSIG",
              "acct.blob.core.windows.net/c/orders",
              "SECRETSIG"),
          new Case("s3://bucket/orders#frag-SECRET", "s3://bucket/orders", "frag-SECRET"),
          new Case(
              "https://admin:hunter2@warehouse.example.com/orders",
              "warehouse.example.com/orders",
              "hunter2"),
          new Case("https://admin:hunter2@host/o?sig=SECRETSIG", "host/o", "hunter2"),
          // A ? or # inside the password used to cut the @ away with the tail, so the credential
          // printed. Userinfo is redacted against the whole value before the query is dropped.
          new Case("https://admin:pa?ss@host/orders", "host/orders", "admin:pa"),
          new Case("https://admin:hunt#er2@host/orders", "host/orders", "admin:hunt"),
        }) {
      var vended =
          new FloecatConnector.VendedStorageCredentials(
              Map.of("s3.access-key-id", "AKIAEXAMPLE"),
              c.prefix(),
              Instant.parse("2030-01-01T00:00:00Z"));

      assertThat(vended.toString())
          .as("%s", c.prefix())
          .contains(c.kept())
          .doesNotContain(c.gone());
    }

    // ADLS and WASB put the container before the @, not a credential. Redacting it would drop the
    // part of the location a scoping mistake shows up in, which is what printing is for.
    for (String container :
        new String[] {
          "abfss://warehouse@acct.dfs.core.windows.net/orders",
          "wasbs://warehouse@acct.blob.core.windows.net/orders"
        }) {
      var vended =
          new FloecatConnector.VendedStorageCredentials(
              Map.of("s3.access-key-id", "AKIAEXAMPLE"),
              container,
              Instant.parse("2030-01-01T00:00:00Z"));
      assertThat(vended.toString()).as("%s", container).contains(container);
    }

    // A colon settles it even on those schemes: user:password@ is not a container.
    var credentialed =
        new FloecatConnector.VendedStorageCredentials(
            Map.of("s3.access-key-id", "AKIAEXAMPLE"),
            "abfss://admin:hunter2@acct.dfs.core.windows.net/orders",
            Instant.parse("2030-01-01T00:00:00Z"));
    assertThat(credentialed.toString()).doesNotContain("hunter2");

    // An @ in the path is part of a key, not a delimiter, and must survive.
    var keyWithAt =
        new FloecatConnector.VendedStorageCredentials(
            Map.of("s3.access-key-id", "AKIAEXAMPLE"),
            "s3://bucket/user@example.com/orders",
            Instant.parse("2030-01-01T00:00:00Z"));
    assertThat(keyWithAt.toString()).contains("s3://bucket/user@example.com/orders");
  }

  @Test
  void aNonPositiveBoundDoesNotThrowFromInsideALogStatement() {
    assertThat(LogSafeText.bounded("anything", 0)).isEqualTo("...(8 chars)");
    assertThat(LogSafeText.bounded("anything", -1)).isEqualTo("...(8 chars)");
    assertThat(LogSafeText.bounded("", 0)).isEmpty();
    assertThat(LogSafeText.bounded(null, 0)).isNull();
  }

  @Test
  void truncationDoesNotCutASurrogatePairInHalf() {
    // The bound is a UTF-16 index. A pair straddling it would leave a lone high surrogate as the
    // last character of the line -- not a character at all, and how it renders is anyone's guess.
    var vended =
        new FloecatConnector.VendedStorageCredentials(
            Map.of("s3.access-key-id", "AKIAEXAMPLE"),
            "a".repeat(255) + "\uD834\uDD1E" + "a".repeat(100),
            Instant.parse("2030-01-01T00:00:00Z"));

    String printed = vended.toString();

    for (int i = 0; i < printed.length(); i++) {
      if (Character.isHighSurrogate(printed.charAt(i))) {
        assertThat(i + 1)
            .as("lone high surrogate at %d in %s", i, printed)
            .isLessThan(printed.length());
        assertThat(Character.isLowSurrogate(printed.charAt(i + 1)))
            .as("unpaired high surrogate at %d", i)
            .isTrue();
      }
    }
  }

  @Test
  void theAcceptedValueDescriptionSeparatesEnablingFromDisabling() {
    // The flat list interleaves them once sorted, so an operator who typed a near-miss cannot see
    // which strings turn vending on -- the thing they were trying to do.
    String description = DatabricksAccessDelegation.acceptedValuesDescription();

    assertThat(description)
        .contains("vended-credentials")
        .contains("to enable")
        .contains("to disable");
    assertThat(description.indexOf("to enable")).isLessThan(description.indexOf("to disable"));
    for (String enabling : new String[] {"true", "1", "yes", "vended-credentials"}) {
      assertThat(description.indexOf(enabling))
          .as(enabling)
          .isLessThan(description.indexOf("to enable"));
    }
    for (String disabling : new String[] {"false", "0", "no", "none"}) {
      assertThat(description.indexOf(disabling))
          .as(disabling)
          .isGreaterThan(description.indexOf("to enable"));
    }
  }

  @Test
  void acceptedValuesListsEveryValueTheCheckAccepts() {
    // The set and the check must not drift apart, in either direction.
    var accepted = DatabricksAccessDelegation.acceptedValues();

    for (String value : accepted) {
      assertThat(DatabricksAccessDelegation.isRecognizedValue(value)).as(value).isTrue();
    }
    assertThat(accepted).contains("vended-credentials", "true", "false", "none");

    // Every value the set names also reaches the operator, spelled the same way.
    String description = DatabricksAccessDelegation.acceptedValuesDescription();
    for (String value : accepted) {
      assertThat(description).as(value).contains(value);
    }

    // The underscore spelling of any of them is accepted too, and the operator is told so. Neither
    // the set nor the description can list both spellings without doubling every entry, so the
    // rule is stated once and the round-trip is asserted here.
    for (String value : accepted) {
      if (value.contains("-")) {
        assertThat(DatabricksAccessDelegation.isRecognizedValue(value.replace('-', '_')))
            .as(value)
            .isTrue();
      }
    }
    assertThat(description).contains("underscore");
    // Underscores fold to hyphens, matching the Iceberg header parser and the REST gateway, so the
    // spelling DuckDB sends is accepted. A misspelling that is not a separator difference is not.
    assertThat(DatabricksAccessDelegation.isRecognizedValue("vended_credentials")).isTrue();
    assertThat(DatabricksAccessDelegation.isRecognizedValue("vended-credential")).isFalse();

    // Absent and blank are accepted: that is what lets a connector omit the property entirely, and
    // rejecting either would fail a legitimate create rather than quietly disabling a feature.
    assertThat(DatabricksAccessDelegation.isRecognizedValue(null)).isTrue();
    assertThat(DatabricksAccessDelegation.isRecognizedValue("")).isTrue();
    assertThat(DatabricksAccessDelegation.isRecognizedValue("   ")).isTrue();
  }

  @Test
  void anOmittedSourceReadsAsUnity() {
    // Nothing requires delta.source: a connector created without it is accepted and treated as
    // Unity, matching DeltaConnectorFactory.selectSource's default.
    var cfg =
        config(ConnectorConfig.Kind.DELTA, Map.of(DatabricksAccessDelegation.VEND_OPTION, "true"));
    assertThat(DatabricksAccessDelegation.declaresVendedCredentials(cfg)).isTrue();
  }

  @Test
  void nonUnityDeltaSourcesIgnoreTheOptIn() {
    for (String source : new String[] {"glue", "filesystem"}) {
      var cfg =
          config(
              ConnectorConfig.Kind.DELTA,
              Map.of("delta.source", source, DatabricksAccessDelegation.VEND_OPTION, "true"));
      assertThat(DatabricksAccessDelegation.declaresVendedCredentials(cfg)).as(source).isFalse();
    }
  }

  @Test
  void absentOrFalsyFlagIsNotDeclared() {
    assertThat(
            DatabricksAccessDelegation.declaresVendedCredentials(
                config(ConnectorConfig.Kind.DELTA, Map.of("delta.source", "unity"))))
        .isFalse();
    assertThat(
            DatabricksAccessDelegation.declaresVendedCredentials(
                config(
                    ConnectorConfig.Kind.DELTA,
                    Map.of(
                        "delta.source", "unity", DatabricksAccessDelegation.VEND_OPTION, "false"))))
        .isFalse();
  }

  @Test
  void icebergKindIgnoresTheDatabricksFlag() {
    // The Databricks opt-in must not fire on an Iceberg connector; that path uses the delegation
    // header instead. A stray copy of the property is simply ignored.
    var cfg =
        config(
            ConnectorConfig.Kind.ICEBERG, Map.of(DatabricksAccessDelegation.VEND_OPTION, "true"));
    assertThat(DatabricksAccessDelegation.declaresVendedCredentials(cfg)).isFalse();
  }

  @Test
  void neutralDispatcherAcceptsBothFormats() {
    // Unity-backed Delta via the opt-in option...
    assertThat(
            SourceCatalogVending.declaresVendedCredentials(
                config(
                    ConnectorConfig.Kind.DELTA,
                    Map.of(
                        "delta.source", "unity", DatabricksAccessDelegation.VEND_OPTION, "true"))))
        .isTrue();
    // ...and Iceberg via its access-delegation header.
    assertThat(
            SourceCatalogVending.declaresVendedCredentials(
                config(
                    ConnectorConfig.Kind.ICEBERG,
                    Map.of(IcebergAccessDelegation.HEADER_PROPERTY, "vended-credentials"))))
        .isTrue();
    // A Delta connector that opted into neither declares nothing.
    assertThat(
            SourceCatalogVending.declaresVendedCredentials(
                config(ConnectorConfig.Kind.DELTA, Map.of())))
        .isFalse();
  }

  @Test
  void glueDeclaresNothing() {
    // Glue has no vending channel at all; the dispatcher must answer false rather than fall through
    // to a Databricks or Iceberg reading of a stray property.
    assertThat(
            SourceCatalogVending.declaresVendedCredentials(
                config(
                    ConnectorConfig.Kind.GLUE,
                    Map.of(
                        DatabricksAccessDelegation.VEND_OPTION,
                        "true",
                        IcebergAccessDelegation.HEADER_PROPERTY,
                        "vended-credentials"))))
        .isFalse();
  }

  @Test
  void aNullConfigDeclaresNothing() {
    assertThat(SourceCatalogVending.declaresVendedCredentials(null)).isFalse();
    assertThat(DatabricksAccessDelegation.declaresVendedCredentials(null)).isFalse();
  }

  /**
   * Declaring vending and applying it at load time are different questions. Only Iceberg REST
   * answers yes to the second: Delta/Unity vend on request and build their S3 client from the
   * connector's own options, so an untouched config carries no storage credentials.
   */
  @Test
  void onlyIcebergAppliesVendedCredentialsWhenLoading() {
    ConnectorConfig unity =
        config(
            ConnectorConfig.Kind.DELTA,
            Map.of("delta.source", "unity", DatabricksAccessDelegation.VEND_OPTION, "true"));
    ConnectorConfig delta =
        config(ConnectorConfig.Kind.DELTA, Map.of(DatabricksAccessDelegation.VEND_OPTION, "true"));
    ConnectorConfig iceberg =
        config(
            ConnectorConfig.Kind.ICEBERG,
            Map.of(IcebergAccessDelegation.HEADER_PROPERTY, "vended-credentials"));

    assertThat(SourceCatalogVending.declaresVendedCredentials(unity)).isTrue();
    assertThat(SourceCatalogVending.clientAppliesVendedCredentials(unity)).isFalse();
    assertThat(SourceCatalogVending.clientAppliesVendedCredentials(delta)).isFalse();
    assertThat(SourceCatalogVending.clientAppliesVendedCredentials(iceberg)).isTrue();
    // Per source, because absorbing is only safe where the client fetches credentials itself. glue
    // and rest are the same REST client here, so their FileIO uses what loadTable vended.
    for (String reachesACatalog : new String[] {"rest", "glue", "GLUE"}) {
      assertThat(
              SourceCatalogVending.clientAppliesVendedCredentials(
                  config(
                      ConnectorConfig.Kind.ICEBERG,
                      Map.of(
                          "iceberg.source",
                          reachesACatalog,
                          IcebergAccessDelegation.HEADER_PROPERTY,
                          "vended-credentials"))))
          .as(reachesACatalog)
          .isTrue();
    }
    // filesystem must not absorb. It has no loadTable to carry credentials, and auth.scheme=none is
    // legal for an s3:// URI, so the untouched config can resolve through the AWS default chain --
    // floecat's own role. Absorbing would turn a missing authority into a read under the wrong
    // principal rather than into an error.
    for (String staticTable : new String[] {"filesystem", "FileSystem", " filesystem "}) {
      assertThat(
              SourceCatalogVending.clientAppliesVendedCredentials(
                  config(
                      ConnectorConfig.Kind.ICEBERG,
                      Map.of(
                          "iceberg.source",
                          staticTable,
                          IcebergAccessDelegation.HEADER_PROPERTY,
                          "vended-credentials"))))
          .as(staticTable)
          .isFalse();
    }
    assertThat(
            SourceCatalogVending.clientAppliesVendedCredentials(
                config(ConnectorConfig.Kind.ICEBERG, Map.of())))
        .isFalse();
    assertThat(SourceCatalogVending.clientAppliesVendedCredentials(null)).isFalse();
  }

  @Test
  void neutralDispatcherDoesNotCrossFormatBoundaries() {
    assertThat(
            SourceCatalogVending.declaresVendedCredentials(
                config(
                    ConnectorConfig.Kind.DELTA,
                    Map.of(
                        "delta.source",
                        "unity",
                        IcebergAccessDelegation.HEADER_PROPERTY,
                        "vended-credentials"))))
        .isFalse();
    assertThat(
            SourceCatalogVending.declaresVendedCredentials(
                config(
                    ConnectorConfig.Kind.ICEBERG,
                    Map.of(DatabricksAccessDelegation.VEND_OPTION, "true"))))
        .isFalse();
  }

  @Test
  void anExpiryTooLargeToStampIsNoExpiryAtAll() {
    // Instant.ofEpochMilli accepts every long, so nothing upstream stops a nonsense value; the
    // service stamps it with Timestamps.fromMillis, which throws past year 9999 -- inside a gRPC
    // handler, where an unrecognized exception is classified retryable. Microseconds in a field
    // documented as milliseconds (1.9e15 -> year 62178) would loop a reconcile job forever.
    for (String tooLarge :
        new String[] {"1900000000000000", "253402300800000", String.valueOf(Long.MAX_VALUE)}) {
      assertThat(FloecatConnector.VendedStorageCredentials.expiryFromEpochMillis(tooLarge))
          .as(tooLarge)
          .isNull();
    }

    // The boundary itself still parses, so the bound cannot quietly become "no expiry ever".
    assertThat(FloecatConnector.VendedStorageCredentials.expiryFromEpochMillis("253402300799999"))
        .isEqualTo(Instant.parse("9999-12-31T23:59:59.999Z"));
    assertThat(FloecatConnector.VendedStorageCredentials.expiryFromEpochMillis("1786000000000"))
        .isEqualTo(Instant.ofEpochMilli(1786000000000L));
  }
}

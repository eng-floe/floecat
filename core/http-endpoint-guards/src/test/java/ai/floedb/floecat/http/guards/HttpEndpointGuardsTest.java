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
package ai.floedb.floecat.http.guards;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import org.junit.jupiter.api.Test;

/**
 * The gates, tested where they live.
 *
 * <p>This module exists because a security control with two callers drifts when it is copied. The
 * same argument reaches its tests: with coverage only in the consumers, a change here is checked by
 * whichever consumer happens to exercise the branch, and {@code mvn -pl} on this module proves
 * nothing. Both of these have since been broken and repaired with the failing test living in
 * another module.
 */
class HttpEndpointGuardsTest {

  private static final String SUBJECT = "Catalog endpoint";

  /**
   * A zone id makes a literal parse as a hostname, which skips the address-class gate entirely.
   * {@code fe80::1%eth0} would otherwise reach a link-local interface with the Authorization header
   * attached.
   */
  @Test
  void refusesAZoneScopedLiteral() {
    assertThatThrownBy(
            () ->
                HttpEndpointGuards.requireAllowedEndpoint(
                    URI.create("https://[fe80::1%25eth0]/api"), SUBJECT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("zone-scoped");
  }

  /**
   * An all-numeric host is not a hostname, and the transport resolves one modulo 2^32 -- so {@code
   * 7147006462} reaches 169.254.169.254, the cloud metadata address.
   */
  @Test
  void refusesAnAllNumericHostThatIsNotAnAddressLiteral() {
    assertThatThrownBy(
            () ->
                HttpEndpointGuards.requireAllowedEndpoint(
                    URI.create("https://7147006462/api"), SUBJECT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("numeric host");
  }

  /** Cleartext is refused, since the Authorization header goes out on every request. */
  @Test
  void refusesCleartextForANonLoopbackHost() {
    assertThatThrownBy(
            () ->
                HttpEndpointGuards.requireAllowedEndpoint(
                    URI.create("http://sharing.example.com/api"), SUBJECT))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatCode(
            () ->
                HttpEndpointGuards.requireAllowedEndpoint(
                    URI.create("https://sharing.example.com/api"), SUBJECT))
        .doesNotThrowAnyException();
  }

  /** Credentials belong in the header, not in a URI that reaches logs and error messages. */
  @Test
  void refusesUserInfo() {
    assertThatThrownBy(
            () ->
                HttpEndpointGuards.requireAllowedEndpoint(
                    URI.create("https://user:pass@sharing.example.com/api"), SUBJECT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("userinfo");
  }

  /**
   * The caller's own token is removed before the character bound, not after.
   *
   * <p>Bounding first cut a token in half before either pass looked for it: the literal no longer
   * matched, and the pattern stops at the first character outside the token68 grammar, so
   * everything between a colon and the cut survived into a message that reaches operator logs.
   */
  @Test
  void redactsACallerTokenThatStraddlesTheBound() {
    String secret = "part1:part2-SECRETSUFFIX";
    String body = "x".repeat(1980) + "Bearer " + secret + " trailing";

    String snippet = HttpResponseSnippets.boundedSingleLine(body, 2_000, secret);

    assertThat(snippet).doesNotContain("part2").doesNotContain("SECRETSUFFIX");
  }

  /**
   * Every occurrence, not only the one inside the bound. A replacement is shorter than what it
   * replaces, so redacting pulls later text forward into the bounded range -- which is why the
   * whole body is scanned rather than a window around the cut.
   */
  @Test
  void redactsAnOccurrenceThatRedactionPullsIntoRange() {
    String secret = "part1:part2-SECRETSUFFIX";
    String body =
        "y".repeat(1900) + "Bearer " + secret + "z".repeat(60) + secret + "z".repeat(60) + secret;

    assertThat(HttpResponseSnippets.boundedSingleLine(body, 2_000, secret))
        .doesNotContain("part1:");
  }

  /** A token shaped the way the grammar says is still covered when the caller names none. */
  @Test
  void redactsABearerTokenWithNoCallerSecretGiven() {
    assertThat(HttpResponseSnippets.boundedSingleLine("echo Bearer abc123.def", 2_000, null))
        .contains("Bearer <redacted>")
        .doesNotContain("abc123");
  }

  /**
   * The parser's own exception is the leak. {@code URI.create} quotes the whole input in its
   * message and again in the nested {@code URISyntaxException}, so an endpoint carrying userinfo --
   * which is what this method exists to refuse -- put that secret into the chain a validation
   * failure logs, on the one path where the value was never parseable enough to be redacted.
   */
  @Test
  void aMalformedEndpointDoesNotCarryItsValueIntoTheCauseChain() {
    String endpoint = "https://user:s3cr3t-PASSWORD@host/%ZZ";

    assertThatThrownBy(() -> HttpEndpointGuards.requireUsableStorageEndpoint(endpoint, SUBJECT))
        .isInstanceOf(IllegalArgumentException.class)
        .satisfies(
            e -> {
              for (Throwable t = e; t != null; t = t.getCause()) {
                assertThat(String.valueOf(t.getMessage()))
                    .doesNotContain("s3cr3t-PASSWORD")
                    .doesNotContain(endpoint);
              }
            });
  }

  /** The position still reaches the message, since it is the half that diagnoses anything. */
  @Test
  void aMalformedEndpointStillSaysWhereItFailed() {
    assertThatThrownBy(
            () -> HttpEndpointGuards.requireUsableStorageEndpoint("https://host/%ZZ", SUBJECT))
        .hasMessageContaining("index");
  }

  /**
   * A token holding a quote or a backslash appears escaped inside a JSON body, so the literal
   * search missed it -- and the pattern missed it too, because a quote is outside the token68
   * alphabet. Both characters are legal in a header value and this control accepts what a header
   * permits.
   */
  @Test
  void redactsATokenThatAJsonBodyEscaped() {
    String secret = "abc\"def";
    String body = "{\"error\":\"upstream sent Authorization: Bearer abc\\\"def\"}";

    assertThat(HttpResponseSnippets.boundedSingleLine(body, 2_000, secret))
        .doesNotContain("abc")
        .doesNotContain("def");
  }

  /**
   * The solidus is the one that matters most: it is inside the token68 alphabet and common in real
   * tokens, so {@code abc/SECRET} echoed as {@code abc\\/SECRET} was missed by the literal search
   * and cut at the backslash by the pattern -- leaving most of the token in the message.
   */
  @Test
  void redactsATokenWhoseSolidusAJsonBodyEscaped() {
    String secret = "abc/SECRET";
    String body = "{\"echo\":\"Authorization: Bearer abc\\/SECRET\"}";

    assertThat(HttpResponseSnippets.boundedSingleLine(body, 2_000, secret))
        .doesNotContain("SECRET");
  }

  /**
   * And an escaping none of the named forms match is withheld rather than partly redacted. A false
   * positive costs a diagnostic; a false negative costs the credential.
   */
  @Test
  void aBodyEscapedInAnUnrecognisedWayIsWithheldRatherThanPartlyRedacted() {
    String secret = "abc/SECRET";
    // Every character escaped individually, which no form in the list constructs.
    String body = "{\"echo\":\"Bearer a\\bc\\/SE\\CRET\"}";

    assertThat(HttpResponseSnippets.boundedSingleLine(body, 2_000, secret))
        .doesNotContain("SECRET")
        .contains("withheld");
  }

  /** The same for a backslash, which a JSON body doubles. */
  @Test
  void redactsATokenWhoseBackslashAJsonBodyDoubled() {
    String secret = "abc\\def";
    String body = "{\"echo\":\"Bearer abc\\\\def\"}";

    assertThat(HttpResponseSnippets.boundedSingleLine(body, 2_000, secret)).doesNotContain("def");
  }

  /**
   * A secret that itself contains a backslash. Stripping backslashes from the body alone destroyed
   * the evidence: the decoded body strips to {@code SECRET} and was compared against the raw {@code
   * \\SECRET}, so nothing matched and nothing was withheld -- while the pattern missed it too, the
   * token beginning with a backslash. The whole credential stayed reconstructable.
   */
  @Test
  void aBackslashBearingSecretRenderedAsAUnicodeEscapeIsWithheld() {
    String secret = "\\SECRET-TAIL";
    String body = "{\"echo\":\"Bearer \\u005cSECRET-TAIL\"}";

    assertThat(HttpResponseSnippets.boundedSingleLine(body, 2_000, secret))
        .doesNotContain("SECRET-TAIL")
        .contains("withheld");
  }

  /**
   * A percent-encoded reflection. Every pass that has to recognise the credential has been defeated
   * in turn by a rendering nobody enumerated -- a colon, an escaped solidus, a backslash-u escape,
   * and this. Recognising the header name instead makes the encoding of the value stop mattering.
   */
  @Test
  void anAuthorizationValueIsRedactedWhateverEncodingItArrivesIn() {
    for (String body :
        new String[] {
          "upstream echoed Authorization: Bearer abc%2FSECRET-TAIL",
          "{\"Authorization\":\"Bearer abc%2FSECRET-TAIL\"}",
          "headers={Authorization=Bearer abc%2FSECRET-TAIL, Accept=*/*}",
          // Deliberately not base64. The scheme is what this case is about -- the pass matches the
          // header name, so the value's encoding is beside the point -- and a fixture that decodes
          // to a credential pair is one a secret scanner reports, correctly, as a credential.
          "Authorization: Basic NOT-A-REAL-CREDENTIAL-FIXTURE"
        }) {
      assertThat(HttpResponseSnippets.bounded(body, 500))
          .as(body)
          .doesNotContain("SECRET-TAIL")
          .doesNotContain("NOT-A-REAL-CREDENTIAL-FIXTURE");
    }
  }

  /** The header name survives, so the message still says what was echoed. */
  @Test
  void redactingTheValueLeavesTheHeaderNameReadable() {
    assertThat(HttpResponseSnippets.bounded("Authorization: Bearer abc%2FSECRET", 500))
        .contains("Authorization")
        .contains("<redacted>");
  }

  /** A token quoted without its header name beside it is still covered by the grammar pass. */
  @Test
  void aBareBearerTokenIsStillRedacted() {
    assertThat(HttpResponseSnippets.bounded("the token was Bearer abc123.def", 500))
        .contains("Bearer <redacted>")
        .doesNotContain("abc123");
  }

  /** A null secret is a caller with nothing to name, not a failure. */
  @Test
  void toleratesANullOrBlankSecret() {
    assertThatCode(
            () -> {
              HttpResponseSnippets.redactSecrets("body", null);
              HttpResponseSnippets.redactSecrets("body", "");
              HttpResponseSnippets.redactSecrets(null, "secret");
            })
        .doesNotThrowAnyException();
  }
}

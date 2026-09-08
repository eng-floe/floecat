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

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * Turns an upstream response body into something safe to put in a failure message.
 *
 * <p>A catalog client sends {@code Authorization} on every request, and a debug handler or a
 * misconfigured gateway will echo request headers back in an error body. That body is interpolated
 * into an exception, which reaches validation responses and operator logs, so without this the
 * tenant's own token travels with the diagnosis.
 *
 * <p>Shared rather than copied because it is a security control with two callers. A second copy
 * drifts, and the copy that drifts is the one nobody reads.
 */
public final class HttpResponseSnippets {

  /**
   * How many encoding layers {@link #couldStillHold} will peel before withholding outright.
   *
   * <p>Bounds the work on a body that decodes to something new indefinitely. Ordinary bodies cost
   * one pass: a text holding no {@code %} and no backslash is returned unchanged by both decoders,
   * so the loop stops on its first comparison.
   */
  private static final int MAX_DECODE_LAYERS = 4;

  private static final Pattern BEARER_TOKEN = Pattern.compile("(?i)bearer\\s+[A-Za-z0-9._~+/=-]+");

  /**
   * The value following an {@code Authorization} header name, whatever shape it is in.
   *
   * <p>Every other pass here has to recognise the credential: the caller's literal, its JSON
   * escapes, the token68 grammar. Each one has been defeated in turn by a rendering nobody
   * enumerated -- a colon, an escaped solidus, a backslash-u escape, and percent-encoding, which is
   * how {@code abc/SECRET} reflected as {@code abc%2FSECRET} kept its tail. This pass recognises
   * the header *name* instead and discards everything after it up to the first delimiter, so the
   * encoding of the value stops mattering. An echoed Authorization header is the case all of this
   * exists for, and its value is never worth reading.
   */
  private static final Pattern AUTHORIZATION_VALUE =
      Pattern.compile("(?i)(authorization[\"']?\\s*[:=]\\s*[\"']?)([^\"',}\\]\\n]*)");

  private HttpResponseSnippets() {}

  /**
   * Reads a response body under a deadline, closing the stream if it stalls.
   *
   * <p>{@code HttpRequest.timeout} only gates receipt of the headers, because {@code ofInputStream}
   * returns as soon as they arrive. A server that sends headers and then stops leaves the read
   * blocked with nothing to interrupt it, which occupies a reconcile worker indefinitely.
   *
   * <p>On a virtual thread of its own rather than the common pool: the read blocks for the whole
   * download, and {@code ForkJoinPool.commonPool()} has parallelism one on a small container, so
   * two concurrent calls queue behind each other and the waiting one fails on this deadline while
   * the server has already answered.
   *
   * <p>Closing the stream is what releases a genuinely stalled read; abandoning the future alone
   * would leave the reader blocked in {@code readNBytes}.
   *
   * @param subject how the upstream is named in the failure, such as {@code "Unity Catalog"}
   */
  public static byte[] readWithin(InputStream stream, int limit, Duration deadline, String subject)
      throws IOException {
    var body = new CompletableFuture<byte[]>();
    Thread.ofVirtual()
        .start(
            () -> {
              try {
                body.complete(stream.readNBytes(limit + 1));
              } catch (Throwable failure) {
                body.completeExceptionally(failure);
              }
            });
    try {
      return body.get(deadline.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException stalled) {
      closeQuietly(stream);
      throw new IOException(subject + " response body stalled after " + deadline, stalled);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      closeQuietly(stream);
      throw new IOException("Interrupted reading the " + subject + " response body", interrupted);
    } catch (ExecutionException failure) {
      Throwable cause = failure.getCause();
      throw cause instanceof IOException io ? io : new IOException(cause);
    }
  }

  private static void closeQuietly(InputStream stream) {
    try {
      stream.close();
    } catch (IOException ignored) {
      // Closing is how a stalled read is released; a failure to close adds nothing to report.
    }
  }

  /** Removes bearer tokens from text on its way into a failure message. */
  public static String redactBearerTokens(String text) {
    if (text == null) {
      return "";
    }
    // The header's whole value first, then the grammar. The first does not care how the value was
    // encoded; the second still covers a token quoted without its header name beside it.
    String withoutHeader = AUTHORIZATION_VALUE.matcher(text).replaceAll("$1<redacted>");
    return BEARER_TOKEN.matcher(withoutHeader).replaceAll("Bearer <redacted>");
  }

  /**
   * {@link #redactBearerTokens} plus the caller's own token, wherever it appears.
   *
   * <p>The pattern above matches the RFC token68 alphabet, which is what a bearer token is supposed
   * to be. A caller may hold one that is not: any character that a header value permits is accepted
   * and sent, so a token carrying a colon matched only up to it and the remainder reached the
   * message unredacted -- defeating the control in exactly the echoed-header case it exists for. A
   * caller that knows its own secret can say so, and then nothing depends on guessing the shape.
   */
  public static String redactSecrets(String text, String secret) {
    if (text == null) {
      return "";
    }
    // The caller's own value first. The pattern below rewrites the part of a token that does match
    // the grammar, which destroys the literal it would then be searched for: redacting the pattern
    // first turned "Bearer part1:part2" into "Bearer <redacted>:part2" and left the suffix behind.
    String withoutSecret = text;
    if (secret != null && !secret.isBlank()) {
      // The escaped form first, then the raw one. A server echoing the Authorization header into a
      // JSON body escapes the quotes and backslashes in it, so a token holding either -- both are
      // legal in a header value, and this client accepts what the header permits -- appears as
      // \" or \\ and the literal search misses it. The pattern misses it too, since a quote is
      // outside the token68 alphabet, so the tail of such a token survived both passes.
      // The escaped renderings first, longest-transformed first, then the raw one. A serializer
      // may escape any subset of these three, so each is tried on its own as well as together:
      // the solidus matters most, because it is inside the token68 alphabet and common in real
      // tokens, so "abc/SECRET" echoed as "abc\\/SECRET" was missed by the literal search and cut
      // at the backslash by the pattern -- leaving most of the token in the message.
      for (String escaped : escapedForms(secret)) {
        withoutSecret = withoutSecret.replace(escaped, "<redacted>");
      }
      withoutSecret = withoutSecret.replace(secret, "<redacted>");
    }
    // Then fail closed. The forms above are the ones worth enumerating; a serializer is free to
    // escape in a way none of them match, so rather than grow that list until one entry is wrong,
    // ask whether anything the body could de-escape to still holds the secret and withhold it if
    // so. A false positive costs a diagnostic; a false negative costs the credential.
    //
    // Asked before the pattern runs, not after: the pattern rewrites the part of a token that does
    // match the grammar, so once "Bearer abc/SECRET" has become "Bearer <redacted>" plus a tail,
    // the literal this looks for no longer exists. The same ordering mistake as bounding before
    // redacting, one layer further in.
    if (secret != null && !secret.isBlank() && couldStillHold(withoutSecret, secret)) {
      return "<withheld: the response echoed the credential>";
    }
    return redactBearerTokens(withoutSecret);
  }

  /** The ways a JSON serializer may render the secret, each escape applied and combined. */
  private static List<String> escapedForms(String secret) {
    LinkedHashSet<String> forms = new LinkedHashSet<>();
    forms.add(secret.replace("\\", "\\\\").replace("\"", "\\\"").replace("/", "\\/"));
    forms.add(secret.replace("\\", "\\\\").replace("\"", "\\\""));
    forms.add(secret.replace("/", "\\/"));
    forms.add(secret.replace("\"", "\\\""));
    forms.add(secret.replace("\\", "\\\\"));
    forms.remove(secret);
    return List.copyOf(forms);
  }

  /**
   * Whether any de-escaping of this text would reveal the secret.
   *
   * <p>Deliberately a superset of a real JSON unescape: every backslash is dropped and every {@code
   * a backslash-u escape} decoded, which can join characters an unescape would not. That direction
   * is the safe one -- it can only withhold a body that was in fact clean, never publish one that
   * was not.
   */
  private static boolean couldStillHold(String text, String secret) {
    // Layer by layer, until the text stops changing. One pass leaves a value encoded twice intact:
    // "abc%252FSECRET" decodes once to "abc%2FSECRET", which holds no secret to find, and the log
    // then carries something a reader decodes again to recover the credential.
    //
    // Both sides are de-escaped the same way. Stripping backslashes from the body alone destroys
    // the evidence for a secret that contains one: "\\SECRET" strips to "SECRET" and would be
    // compared against the raw "\\SECRET". Comparing like with like errs towards withholding.
    String bare = decodeUnicodeEscapes(secret).replace("\\", "");
    String current = text;
    for (int layer = 0; layer < MAX_DECODE_LAYERS; layer++) {
      String stripped = current.replace("\\", "");
      if (stripped.contains(secret) || stripped.contains(bare)) {
        return true;
      }
      String next = decodePercentEscapes(decodeUnicodeEscapes(current));
      if (next.equals(current)) {
        return false;
      }
      current = next;
    }
    // Still decoding to something new after the bound. Whatever that is, it is not a body worth
    // publishing to find out.
    return true;
  }

  /** Percent escapes decoded, so the net sees a value a proxy reflected in encoded form. */
  private static String decodePercentEscapes(String text) {
    if (text.indexOf('%') < 0) {
      return text;
    }
    StringBuilder out = new StringBuilder(text.length());
    for (int i = 0; i < text.length(); i++) {
      if (text.charAt(i) == '%' && i + 2 < text.length()) {
        try {
          out.append((char) Integer.parseInt(text.substring(i + 1, i + 3), 16));
          i += 2;
          continue;
        } catch (NumberFormatException notAnEscape) {
          // Not an escape after all; copy the percent verbatim.
        }
      }
      out.append(text.charAt(i));
    }
    return out.toString();
  }

  private static String decodeUnicodeEscapes(String text) {
    if (text.indexOf('\\') < 0) {
      return text;
    }
    StringBuilder out = new StringBuilder(text.length());
    for (int i = 0; i < text.length(); i++) {
      if (text.charAt(i) == '\\'
          && i + 5 < text.length()
          && (text.charAt(i + 1) == 'u' || text.charAt(i + 1) == 'U')) {
        try {
          out.append((char) Integer.parseInt(text.substring(i + 2, i + 6), 16));
          i += 5;
          continue;
        } catch (NumberFormatException notAnEscape) {
          // Not an escape after all; fall through and copy the backslash verbatim.
        }
      }
      out.append(text.charAt(i));
    }
    return out.toString();
  }

  /**
   * A bounded, redacted snippet of a response body.
   *
   * <p>Redacted first and bounded after. The reverse cut a secret in half before anything looked
   * for it: the remaining fragment no longer matched a caller's literal token, and for a token
   * outside the {@code token68} grammar the pattern stopped at the first character it does not
   * accept, so the suffix between there and the bound survived into the snippet.
   *
   * <p>Redacting only a window around the bound is not enough either, which is why the whole body
   * is scanned. A replacement is shorter than what it replaces, so redacting pulls later text
   * forward into the bounded range -- text that a window would not have reached, and that can hold
   * another occurrence of the same secret.
   *
   * <p>The cost is a scan of the whole body on a failure path. It is bounded by the caller's own
   * response cap, and a failure that is about to be logged is not where a copy matters.
   */
  public static String bounded(String value, int maxChars) {
    return bounded(value, maxChars, null);
  }

  /** {@link #bounded} that also removes the caller's own token, by value. */
  public static String bounded(String value, int maxChars, String secret) {
    if (value == null) {
      return "";
    }
    String redacted = redactSecrets(value, secret);
    return redacted.substring(0, Math.min(redacted.length(), maxChars));
  }

  /**
   * {@link #bounded} for a body that reaches a single-line message, with an ellipsis where the
   * bound cut it.
   */
  public static String boundedSingleLine(String body, int maxChars) {
    return boundedSingleLine(body, maxChars, null);
  }

  /** {@link #boundedSingleLine} that also removes the caller's own token. */
  public static String boundedSingleLine(String body, int maxChars, String secret) {
    if (body == null) {
      return "";
    }
    String flattened = body.replace('\n', ' ').replace('\r', ' ').trim();
    // Redact, then bound. See bounded() for why the order is this way round and why the whole body
    // is scanned rather than a window at the cut.
    String redacted = redactSecrets(flattened, secret);
    return redacted.length() <= maxChars ? redacted : redacted.substring(0, maxChars) + "...";
  }
}

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

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/** Preparing a value that came from a catalog for a log line or an exception message. */
public final class LogSafeText {

  /**
   * Everything a log renderer may treat as ending a line.
   *
   * <p>Wider than {@code \p{Cntrl}}, which is the POSIX ASCII class and matches neither U+0085 nor
   * the Unicode separators U+2028 and U+2029. Those are line terminators to a Unicode-aware
   * renderer, so leaving them through would let a catalog forge a log line through the one path
   * that exists to stop it.
   */
  private static final Pattern LINE_BREAKING = Pattern.compile("[\\p{Gc=Cc}\\p{Gc=Zl}\\p{Gc=Zp}]");

  /**
   * Schemes whose authority uses {@code @} to separate a container from the account rather than
   * credentials from a host.
   *
   * <p>{@code abfss://warehouse@acct.dfs.core.windows.net/orders} names the container {@code
   * warehouse}. Redacting that drops the part of the location a scoping mistake shows up in, which
   * is the reason this value is printed rather than hidden, and protects nothing.
   */
  private static final Set<String> CONTAINER_AT_SCHEMES = Set.of("abfs", "abfss", "wasb", "wasbs");

  private LogSafeText() {}

  /**
   * One line, no longer than {@code maxChars}, with a marker when anything was dropped.
   *
   * <p>For any value a catalog supplied: a storage prefix, an OAuth error code, an unparseable
   * expiry. None of them carry a length limit or a character restriction, and all of them reach a
   * log line -- where a newline forges a second entry and a megabyte fills the file.
   *
   * <p>A truncated value keeps the original length, which is the difference between "the catalog
   * sent a slightly long prefix" and "the catalog sent a megabyte". {@code null} comes back as
   * {@code null} so a caller can still tell absent from present.
   */
  public static String bounded(String value, int maxChars) {
    if (value == null) {
      return null;
    }
    String flat = LINE_BREAKING.matcher(value).replaceAll("?");
    if (flat.length() <= maxChars) {
      return flat;
    }
    // A helper that exists to be the safe path must not be the thing that throws from inside a log
    // statement. No caller asks for a non-positive bound today; one that did would get charAt(-1).
    if (maxChars <= 0) {
      return "...(" + flat.length() + " chars)";
    }
    // maxChars is a UTF-16 index. Cutting between the halves of a surrogate pair would leave a lone
    // high surrogate at the end of the line, so back off one when that is where the cut lands.
    int end = Character.isHighSurrogate(flat.charAt(maxChars - 1)) ? maxChars - 1 : maxChars;
    return flat.substring(0, end) + "...(" + flat.length() + " chars)";
  }

  /**
   * A storage location rendered without the parts of a URI that can carry a secret.
   *
   * <p>A prefix comes from the catalog, and nothing constrains it to a bare {@code
   * s3://bucket/key}. A query or fragment on one is a presigned signature or a SAS token, and
   * userinfo is a password; none of the three say anything about <em>where</em> the credential is
   * scoped, which is the only question this value is printed to answer. What is dropped is marked,
   * so a reader can tell a prefix that carried a signature from one that did not.
   */
  public static String location(String value, int maxChars) {
    if (value == null) {
      return null;
    }
    // Userinfo goes first, against the whole value. Cutting the query before looking for the @
    // loses the @ whenever a ? or # sits inside the password, and the credential then prints.
    String withoutUserInfo = redactUserInfo(value);
    int cut = withoutUserInfo.length();
    char cutAt = 0;
    for (int i = 0; i < withoutUserInfo.length(); i++) {
      char c = withoutUserInfo.charAt(i);
      if (c == '?' || c == '#') {
        cut = i;
        cutAt = c;
        break;
      }
    }
    String rendered = bounded(withoutUserInfo.substring(0, cut), maxChars);
    return cutAt == 0 ? rendered : rendered + cutAt + "<redacted>";
  }

  /**
   * The value with any userinfo in its authority replaced.
   *
   * <p>The authority runs to the next {@code /} and no further. It deliberately does not stop at a
   * {@code ?} or {@code #}: those are illegal there unencoded, so one appearing means the value is
   * malformed, and stopping would drop the {@code @} that marks the credential. The cost is that an
   * {@code @} inside a query on a path-less URL is read as userinfo and redacted -- over-redaction
   * on a shape no storage prefix has, which is the direction to err in.
   */
  private static String redactUserInfo(String value) {
    int marker = value.indexOf("//");
    if (marker < 0) {
      return value;
    }
    int authority = marker + 2;
    int endOfAuthority = authority;
    while (endOfAuthority < value.length() && value.charAt(endOfAuthority) != '/') {
      endOfAuthority++;
    }
    // The last @ inside the authority is the userinfo delimiter; a later one is part of a key.
    int at = value.lastIndexOf('@', endOfAuthority - 1);
    if (at < authority || !isUserInfo(value, marker, authority, at)) {
      return value;
    }
    return value.substring(0, authority) + "<redacted>@" + value.substring(at + 1);
  }

  /**
   * Whether what precedes {@code @} in the authority is a credential rather than a container.
   *
   * <p>A colon inside it settles the question: {@code user:password@} is nothing else. Without one
   * the shape is ambiguous, so the scheme decides, and anything unrecognised is treated as a
   * credential -- a redacted container costs a diagnostic, a printed token costs the token.
   */
  private static boolean isUserInfo(String value, int marker, int authority, int at) {
    if (value.lastIndexOf(':', at) >= authority) {
      return true;
    }
    String scheme =
        marker >= 1 && value.charAt(marker - 1) == ':'
            ? value.substring(0, marker - 1).toLowerCase(Locale.ROOT)
            : "";
    return !CONTAINER_AT_SCHEMES.contains(scheme);
  }
}

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

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Short-lived, table-scoped credentials returned by an upstream catalog protocol. */
public record VendedStorageCredentials(
    Map<String, String> properties, String scopePrefix, Optional<Instant> expiresAt) {
  private static final int MAX_PREFIX_CHARS = 256;

  public VendedStorageCredentials {
    properties = Map.copyOf(Objects.requireNonNull(properties, "properties"));
    if (properties.isEmpty()) {
      throw new IllegalArgumentException("properties must not be empty");
    }
    scopePrefix = Objects.requireNonNull(scopePrefix, "scopePrefix");
    expiresAt = Objects.requireNonNull(expiresAt, "expiresAt");
  }

  /**
   * Whether these credentials reach {@code location}.
   *
   * <p>The scope is what the catalog authorized, and it is not necessarily the location a caller
   * wants to read: a table can hold data outside its own table prefix.
   *
   * <p>Not a guard every caller applies. The one that hands these credentials on for a specific
   * location deliberately does not: it stamps the location the caller was authorized for and logs a
   * disjoint scope rather than refusing, because that vend is reached only once no storage
   * authority covers the read, so refusing guarantees failure while proceeding lets the object
   * store -- which enforces the real grant -- decide. Its only use today is a provider checking its
   * own answer before a validation read.
   */
  public boolean covers(String location) {
    return StorageLocations.covers(scopePrefix, location);
  }

  /**
   * Key names only. The properties carry live storage credentials and the generated form prints
   * them.
   *
   * <p>The scope prefix is printed. It is a storage location rather than a credential, and it
   * decides how a vended credential gets scoped, so redacting it removes the answer to the question
   * being asked when scoping is wrong. Matches {@code
   * FloecatConnector.VendedStorageCredentials.toString}, which states the same policy for the
   * connector-side record of the same name.
   */
  @Override
  public String toString() {
    return "VendedStorageCredentials[propertyKeys="
        + NonSecretCatalogConfig.propertyKeys(properties)
        + ", scopePrefix="
        + displayPrefix(scopePrefix)
        + ", expiresAt="
        + expiresAt
        + "]";
  }

  /**
   * A prefix bounded and flattened for a log line. {@code IcebergRestCatalogClient} passes the
   * upstream {@code loadTable} response's prefix straight through, so the value carries no length
   * limit and no character restriction: a newline in it forges a second log line, and a megabyte of
   * it fills a log file.
   *
   * <p>Duplicated in {@code LogSafeText.location} rather than shared: this module deliberately has
   * no floecat dependencies, and the connector SPI pulls in proto and types. The two must agree on
   * five rules -- drop userinfo but keep an ADLS container, drop query and fragment, flatten line
   * breaks, bound the length without splitting a surrogate pair -- and each side tests all of them.
   */
  private static String displayPrefix(String prefix) {
    // Callers spell "no prefix" as "" -- IcebergRestCatalogClient does exactly that. Printing an
    // empty slot loses the distinction this field is printed to make, which the sibling record
    // marks the same way.
    if (prefix.isBlank()) {
      return "<absent>";
    }
    // A query or fragment on a storage prefix is a presigned signature or a SAS token, and userinfo
    // is a password. None of the three say where the credential is scoped, which is the only reason
    // this value is printed. What was dropped is marked so a reader can tell the two apart.
    // Userinfo goes first, against the whole value: cutting the query before looking for the @
    // loses the @ whenever a ? or # sits inside the password, and the credential then prints.
    String kept = redactUserInfo(prefix);
    int cut = kept.length();
    char cutAt = 0;
    for (int i = 0; i < kept.length(); i++) {
      char c = kept.charAt(i);
      if (c == '?' || c == '#') {
        cut = i;
        cutAt = c;
        break;
      }
    }
    kept = kept.substring(0, cut);
    // Wider than \p{Cntrl}, the POSIX ASCII class, which matches neither U+0085 nor the Unicode
    // separators U+2028 and U+2029 -- all three end a line for a Unicode-aware renderer.
    String flat = kept.replaceAll("[\\p{Gc=Cc}\\p{Gc=Zl}\\p{Gc=Zp}]", "?");
    String bounded = flat;
    if (flat.length() > MAX_PREFIX_CHARS) {
      // MAX_PREFIX_CHARS is a UTF-16 index. Cutting between the halves of a surrogate pair would
      // end the line on half a character, so back off one when that is where the cut lands.
      int end =
          Character.isHighSurrogate(flat.charAt(MAX_PREFIX_CHARS - 1))
              ? MAX_PREFIX_CHARS - 1
              : MAX_PREFIX_CHARS;
      bounded = flat.substring(0, end) + "...(" + flat.length() + " chars)";
    }
    return cutAt == 0 ? bounded : bounded + cutAt + "<redacted>";
  }

  /**
   * The value with any userinfo in its authority replaced.
   *
   * <p>The authority runs to the next {@code /} and no further -- deliberately not stopping at a
   * {@code ?} or {@code #}, which are illegal there unencoded, so one appearing means the value is
   * malformed and stopping would drop the {@code @} marking the credential.
   */
  private static String redactUserInfo(String prefix) {
    int marker = prefix.indexOf("//");
    if (marker < 0) {
      return prefix;
    }
    int authority = marker + 2;
    int endOfAuthority = authority;
    while (endOfAuthority < prefix.length() && prefix.charAt(endOfAuthority) != '/') {
      endOfAuthority++;
    }
    // The last @ inside the authority is the userinfo delimiter; a later one is part of a key.
    int at = prefix.lastIndexOf('@', endOfAuthority - 1);
    if (at < authority || !isUserInfo(prefix, marker, authority, at)) {
      return prefix;
    }
    return prefix.substring(0, authority) + "<redacted>@" + prefix.substring(at + 1);
  }

  /**
   * Whether what precedes {@code @} in the authority is a credential rather than a container.
   *
   * <p>{@code abfss://warehouse@acct.dfs.core.windows.net/orders} names the container, which is the
   * part a scoping mistake shows up in. A colon settles it -- {@code user:password@} is nothing
   * else -- and without one the scheme decides, anything unrecognised counting as a credential.
   */
  private static boolean isUserInfo(String value, int marker, int authority, int at) {
    if (value.lastIndexOf(':', at) >= authority) {
      return true;
    }
    String scheme =
        marker >= 1 && value.charAt(marker - 1) == ':'
            ? value.substring(0, marker - 1).toLowerCase(java.util.Locale.ROOT)
            : "";
    return !Set.of("abfs", "abfss", "wasb", "wasbs").contains(scheme);
  }
}

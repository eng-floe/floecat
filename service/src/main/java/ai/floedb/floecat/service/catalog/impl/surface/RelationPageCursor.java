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
package ai.floedb.floecat.service.catalog.impl.surface;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.PAGE_TOKEN_INVALID;

import ai.floedb.floecat.catalog.rpc.ListRelationsRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.service.common.PageTokens;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Where a relation listing resumes: the segment to continue in, that segment's own page token, and
 * the total counted on the first page.
 *
 * <p>Wrapped in one opaque {@code rel:} token so a typed pager's token never reaches a client.
 * {@code segmentKey} keysets on the segment rather than its position, so a namespace dropped
 * between pages does not void the token.
 */
record RelationPageCursor(
    String scopeFingerprint, String segmentKey, int total, String innerToken) {

  private static final String TOKEN_PREFIX = "rel:";
  private static final char FIELD_SEPARATOR = '|';
  private static final int FIELDS = 4;

  /** A total of -1 means "not counted yet". */
  static final int UNCOUNTED = -1;

  static RelationPageCursor start() {
    return new RelationPageCursor("", "", UNCOUNTED, "");
  }

  String encode() {
    return PageTokens.encode(
        TOKEN_PREFIX,
        scopeFingerprint
            + FIELD_SEPARATOR
            + total
            + FIELD_SEPARATOR
            + encodeField(segmentKey)
            + FIELD_SEPARATOR
            + innerToken);
  }

  /**
   * A segment key carries namespace names, so it can contain the field separator. Base64url has no
   * separator in its alphabet. The fingerprint is hex and the total is digits; the inner token is
   * last, so the split's limit absorbs any separator it carries.
   */
  private static String encodeField(String value) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  private static String decodeField(String value, String token, String corr) {
    try {
      return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    } catch (IllegalArgumentException badToken) {
      throw invalid(token, corr);
    }
  }

  static RelationPageCursor decode(String token, String corr) {
    if (token == null || token.isBlank()) {
      return start();
    }
    if (!token.startsWith(TOKEN_PREFIX)) {
      throw invalid(token, corr);
    }
    String[] parts =
        PageTokens.decode(TOKEN_PREFIX, token, corr).split("\\" + FIELD_SEPARATOR, FIELDS);
    if (parts.length != FIELDS) {
      throw invalid(token, corr);
    }
    try {
      return new RelationPageCursor(
          parts[0], decodeField(parts[2], token, corr), Integer.parseInt(parts[1]), parts[3]);
    } catch (NumberFormatException notANumber) {
      throw invalid(token, corr);
    }
  }

  /** Binds a token to the request that minted it: a different scope cannot resume on it. */
  static String scopeFingerprint(
      ListRelationsRequest request,
      List<ResourceKind> kinds,
      String accountId,
      CatalogContext context) {
    StringBuilder canonical = new StringBuilder(accountId).append('\0');
    if (request.hasCatalogId()) {
      appendScope(canonical, "catalog", request.getCatalogId());
    } else {
      appendScope(canonical, "namespace", request.getNamespaceId());
    }
    canonical
        .append('\0')
        .append(request.getRecursive())
        .append('\0')
        .append(request.getIncludeSchema())
        .append('\0')
        .append(request.getIncludeStatus())
        .append('\0')
        .append(request.getIncludeTotal());
    appendContext(canonical, context);
    for (ResourceKind kind : kinds) {
      canonical.append('\0').append(kind.getNumber());
    }
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256")
                  .digest(canonical.toString().getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new AssertionError("SHA-256 is required", impossible);
    }
  }

  private static void appendContext(StringBuilder canonical, CatalogContext context) {
    canonical
        .append('\0')
        .append(context.environment().normalizedKind())
        .append('\0')
        .append(context.environment().normalizedVersion())
        .append('\0')
        .append(context.engine().normalizedKind())
        .append('\0')
        .append(context.engine().normalizedVersion());
  }

  private static void appendScope(StringBuilder canonical, String type, ResourceId id) {
    canonical
        .append(type)
        .append('\0')
        .append(id.getAccountId())
        .append('\0')
        .append(id.getId())
        .append('\0')
        .append(id.getKindValue());
  }

  void requireScope(String expected, String token, String corr) {
    if (!scopeFingerprint.equals(expected)) {
      throw invalid(token, corr);
    }
  }

  private static RuntimeException invalid(String token, String corr) {
    return GrpcErrors.invalidArgument(corr, PAGE_TOKEN_INVALID, Map.of("page_token", token));
  }
}

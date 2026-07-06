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

package ai.floedb.floecat.service.common;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.PAGE_TOKEN_INVALID;

import ai.floedb.floecat.service.error.impl.GrpcErrors;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/**
 * Phase-scoped page tokens shared by the Catalog Surface pagers and the MetaGraph phased prefix
 * listing. A token is a phase {@code prefix} followed by the base64url encoding of the "resume
 * after" payload (empty payload → just the prefix). Decoding strips the prefix and base64-decodes,
 * rejecting a structurally invalid token with {@code PAGE_TOKEN_INVALID}.
 */
public final class PageTokens {

  private PageTokens() {}

  /** Encodes {@code prefix + base64url(resumeAfter)}; a blank payload yields just the prefix. */
  public static String encode(String prefix, String resumeAfter) {
    if (resumeAfter == null || resumeAfter.isBlank()) {
      return prefix;
    }
    return prefix
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(resumeAfter.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Decodes a token produced by {@link #encode}. Returns "" for a null/blank token, one that does
   * not carry {@code prefix}, or a prefix-only token; throws {@code PAGE_TOKEN_INVALID} when the
   * payload after the prefix is not valid base64url.
   */
  public static String decode(String prefix, String token, String corr) {
    if (token == null || token.isBlank() || !token.startsWith(prefix)) {
      return "";
    }
    if (token.length() == prefix.length()) {
      return "";
    }
    var payload = token.substring(prefix.length());
    try {
      return new String(Base64.getUrlDecoder().decode(payload), StandardCharsets.UTF_8);
    } catch (IllegalArgumentException badToken) {
      throw GrpcErrors.invalidArgument(corr, PAGE_TOKEN_INVALID, Map.of("page_token", token));
    }
  }
}

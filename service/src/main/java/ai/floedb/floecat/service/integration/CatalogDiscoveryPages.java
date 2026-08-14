/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.integration;

import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/** Request-bound offset pages for provider inventories that expose list rather than cursor APIs. */
final class CatalogDiscoveryPages {
  private static final String VERSION = "cd1";
  private static final int DEFAULT_PAGE_SIZE = 50;
  private static final int MAX_PAGE_SIZE = 200;

  private CatalogDiscoveryPages() {}

  static <T> Page<T> page(
      List<T> values, PageRequest request, String context, String correlationId) {
    int requested = request == null ? 0 : request.getPageSize();
    int pageSize = requested <= 0 ? DEFAULT_PAGE_SIZE : Math.min(requested, MAX_PAGE_SIZE);
    String expectedContext = contextHash(context);
    int offset =
        decodeOffset(request == null ? "" : request.getPageToken(), expectedContext, correlationId);
    if (offset > values.size()) {
      throw invalidToken(correlationId);
    }
    int end = Math.min(values.size(), offset + pageSize);
    String next = end < values.size() ? VERSION + "." + expectedContext + "." + end : "";
    return new Page<>(values.subList(offset, end), next, values.size());
  }

  private static int decodeOffset(String token, String expectedContext, String correlationId) {
    if (token == null || token.isBlank()) {
      return 0;
    }
    String[] parts = token.split("\\.", -1);
    if (parts.length != 3 || !VERSION.equals(parts[0]) || !expectedContext.equals(parts[1])) {
      throw invalidToken(correlationId);
    }
    try {
      int offset = Integer.parseInt(parts[2]);
      if (offset <= 0) {
        throw invalidToken(correlationId);
      }
      return offset;
    } catch (NumberFormatException invalid) {
      throw invalidToken(correlationId);
    }
  }

  private static String contextHash(String context) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(context.getBytes(StandardCharsets.UTF_8));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest).substring(0, 22);
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  private static io.grpc.StatusRuntimeException invalidToken(String correlationId) {
    return GrpcErrors.invalidArgument(correlationId, null, Map.of("field", "page.page_token"));
  }

  record Page<T>(List<T> values, String nextToken, int totalSize) {
    Page {
      values = List.copyOf(values);
    }
  }
}

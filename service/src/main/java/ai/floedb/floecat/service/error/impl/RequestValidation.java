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

package ai.floedb.floecat.service.error.impl;

import io.grpc.Metadata;
import java.util.LinkedHashMap;
import java.util.Map;

public final class RequestValidation {
  private RequestValidation() {}

  public static String requireHeader(
      Metadata headers,
      Metadata.Key<String> key,
      String correlationId,
      String messageKey,
      String fieldName) {
    String value = headers == null ? null : headers.get(key);
    return requireNonBlank(value, fieldName, correlationId, messageKey);
  }

  public static String requireNonBlank(
      String value, String fieldName, String correlationId, String messageKey) {
    if (value == null || value.isBlank()) {
      throwInvalidArgument(fieldName, correlationId, messageKey);
    }
    return value.trim();
  }

  private static void throwInvalidArgument(
      String fieldName, String correlationId, String messageKey) {
    Map<String, String> params = new LinkedHashMap<>();
    params.put("field", fieldName);
    throw GrpcErrors.invalidArgument(correlationId, fallbackMessageKey(messageKey), params, null);
  }

  private static GeneratedErrorMessages.MessageKey fallbackMessageKey(String messageKey) {
    if (messageKey != null && !messageKey.isBlank()) {
      GeneratedErrorMessages.MessageKey key = GeneratedErrorMessages.findBySuffix(messageKey);
      if (key != null) {
        return key;
      }
    }
    return GeneratedErrorMessages.MessageKey.FIELD;
  }
}

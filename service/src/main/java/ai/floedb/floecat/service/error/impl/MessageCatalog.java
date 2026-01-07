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

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

public final class MessageCatalog {
  private final ResourceBundle bundle;

  public MessageCatalog(Locale locale) {
    this.bundle = ResourceBundle.getBundle("errors", locale);
  }

  public String render(Error e) {
    String base = e.getCode().name();
    String key = !e.getMessageKey().isBlank() ? base + "." + e.getMessageKey() : base;

    String template =
        bundle.containsKey(key)
            ? bundle.getString(key)
            : (bundle.containsKey(base) ? bundle.getString(base) : defaultTemplate(e.getCode()));

    return format(template, e.getParamsMap());
  }

  private static String defaultTemplate(ErrorCode code) {
    return switch (code) {
      case MC_ABORT_RETRYABLE -> "Conflict detected, retry.";
      case MC_NOT_FOUND -> "The {resource} was not found: {id}.";
      case MC_INVALID_ARGUMENT -> "Invalid value for {field}.";
      case MC_PRECONDITION_FAILED -> "Precondition failed: {reason}.";
      case MC_CONFLICT -> "Conflict: {detail}.";
      case MC_RATE_LIMITED -> "Too many requests. Try again later.";
      case MC_UNAUTHENTICATED -> "Authentication required.";
      case MC_PERMISSION_DENIED -> "You do not have permission to perform this operation.";
      case MC_UNAVAILABLE -> "Service temporarily unavailable.";
      case MC_TIMEOUT -> "The operation timed out.";
      case MC_INTERNAL -> "Unexpected error.";
      case MC_CANCELLED -> "Request cancelled.";
      case MC_SNAPSHOT_EXPIRED -> "Snapshot is no longer available.";
      case MC_UNSPECIFIED, UNRECOGNIZED -> "An error occurred.";
    };
  }

  private static String format(String template, Map<String, String> params) {
    String formattedMessage = template;
    for (var e : params.entrySet()) {
      formattedMessage = formattedMessage.replace("{" + e.getKey() + "}", e.getValue());
    }
    return formattedMessage;
  }
}

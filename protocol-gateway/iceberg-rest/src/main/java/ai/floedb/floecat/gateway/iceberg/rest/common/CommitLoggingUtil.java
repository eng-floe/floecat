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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import java.nio.charset.StandardCharsets;

final class CommitLoggingUtil {
  static final int MAX_LOG_BYTES = 64 * 1024;

  private CommitLoggingUtil() {}

  static boolean isCommitTrafficPath(String method, String path) {
    if (!"POST".equalsIgnoreCase(method) || path == null) {
      return false;
    }
    return path.contains("/transactions/commit")
        || (path.contains("/namespaces/") && path.contains("/tables/") && !path.endsWith("/plan"));
  }

  static String truncateUtf8(String value, int limitBytes) {
    if (value == null) {
      return "<empty>";
    }
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    if (bytes.length <= limitBytes) {
      return value;
    }
    return new String(bytes, 0, limitBytes, StandardCharsets.UTF_8) + "...(truncated)";
  }
}

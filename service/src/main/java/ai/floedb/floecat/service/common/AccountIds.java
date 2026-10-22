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

import java.util.UUID;

public final class AccountIds {
  private AccountIds() {}

  public static String randomAccountId() {
    return UUID.randomUUID().toString();
  }

  /** Account identifiers are embedded as canonical storage-key path segments. */
  public static boolean isStoragePathSegment(String value) {
    if (value == null || value.isBlank()) {
      return false;
    }
    for (int index = 0; index < value.length(); index++) {
      char character = value.charAt(index);
      if ((character >= 'a' && character <= 'z')
          || (character >= 'A' && character <= 'Z')
          || (character >= '0' && character <= '9')
          || character == '-'
          || character == '_'
          || character == '.') {
        continue;
      }
      return false;
    }
    return true;
  }
}

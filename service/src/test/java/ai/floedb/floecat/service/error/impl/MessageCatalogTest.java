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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.floedb.floecat.common.rpc.ErrorCode;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import org.junit.jupiter.api.Test;

class MessageCatalogTest {
  @Test
  void everyErrorCodeRequiresAPropertyEntry() {
    final ResourceBundle bundle;
    try {
      bundle = ResourceBundle.getBundle("errors", Locale.ENGLISH);
    } catch (MissingResourceException e) {
      fail("errors_en.properties missing or unreadable", e);
      return;
    }
    for (ErrorCode code : ErrorCode.values()) {
      if (code == ErrorCode.UNRECOGNIZED) {
        continue;
      }
      assertTrue(
          bundle.containsKey(code.name()), () -> code.name() + " missing in errors_en.properties");
    }
  }
}

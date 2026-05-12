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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Map;
import org.junit.jupiter.api.Test;

class TableRegisterRequestBuilderTest {

  @Test
  void mergeImportedPropertiesDoesNotPersistSecretBearingRegisterProps() {
    TableRegisterRequestBuilder builder = new TableRegisterRequestBuilder();

    Map<String, String> merged =
        builder.mergeImportedProperties(
            Map.of("existing", "value"),
            null,
            Map.of(
                "s3.endpoint", "http://localhost:4566",
                "s3.access-key-id", "akid",
                "s3.secret-access-key", "secret",
                "s3.session-token", "session",
                "format-version", "2"));

    assertEquals("value", merged.get("existing"));
    assertEquals("http://localhost:4566", merged.get("s3.endpoint"));
    assertEquals("2", merged.get("format-version"));
    assertFalse(merged.containsKey("metadata-location"));
    assertFalse(merged.containsKey("s3.access-key-id"));
    assertFalse(merged.containsKey("s3.secret-access-key"));
    assertFalse(merged.containsKey("s3.session-token"));
  }
}

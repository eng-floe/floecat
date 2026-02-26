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

package ai.floedb.floecat.gateway.iceberg.rest.api.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

class TableRequestsTest {

  @Test
  void commitRequestAcceptsOptionalIdentifierField() throws Exception {
    ObjectMapper mapper =
        new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    String payload =
        """
        {
          "identifier": { "namespace": ["iceberg"], "name": "duckdb_mutation_smoke" },
          "requirements": [],
          "updates": [{ "action": "set-location", "location": "s3://bucket/path/" }]
        }
        """;

    TableRequests.Commit commit = mapper.readValue(payload, TableRequests.Commit.class);

    assertNotNull(commit.identifier());
    assertEquals(List.of("iceberg"), commit.identifier().namespace());
    assertEquals("duckdb_mutation_smoke", commit.identifier().name());
    assertEquals(1, commit.updates().size());
  }
}

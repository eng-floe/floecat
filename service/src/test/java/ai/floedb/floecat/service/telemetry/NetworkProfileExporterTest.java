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

package ai.floedb.floecat.service.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class NetworkProfileExporterTest {
  @Test
  void appendsIngestToBasePathAndEncodesProfileNameOnce() {
    URI uri = NetworkProfileExporter.ingestUri("http://pyroscope:4040/profiles/", 100, 160);

    assertEquals("/profiles/ingest", uri.getPath());
    assertTrue(
        uri.getRawQuery().contains("name=floecat-service%7Bservice_name%3Dfloecat-service%7D"));
    assertEquals(
        "name=floecat-service{service_name=floecat-service}&from=100&until=160",
        URLDecoder.decode(uri.getRawQuery(), StandardCharsets.UTF_8));
  }
}

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

package ai.floedb.floecat.service.context.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.scanner.utils.EnvironmentContext;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

final class InboundCallContextHelperTest {

  @Test
  void resolvesEnvironmentAlongsideEngineFromHeaders() {
    InboundCallContextHelper helper =
        new InboundCallContextHelper(
            null,
            null,
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            "dev",
            "sub",
            "roles");
    Map<String, String> headers =
        Map.of(
            "x-engine-kind",
            "duckdb",
            "x-engine-version",
            "1.0",
            EnvironmentContext.HEADER_KIND,
            "floe",
            EnvironmentContext.HEADER_VERSION,
            "3.1");

    var resolved = helper.resolve(headers::get, true);

    assertThat(resolved.engineContext().normalizedKind()).isEqualTo("duckdb");
    assertThat(resolved.environmentContext().normalizedKind()).isEqualTo("floe");
    assertThat(resolved.environmentContext().normalizedVersion()).isEqualTo("3.1");
    assertThat(resolved.catalogContext().engine().normalizedKind()).isEqualTo("duckdb");
    assertThat(resolved.catalogContext().environment().normalizedKind()).isEqualTo("floe");
  }
}

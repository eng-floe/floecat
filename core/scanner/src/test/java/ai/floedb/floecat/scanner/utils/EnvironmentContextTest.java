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

package ai.floedb.floecat.scanner.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

final class EnvironmentContextTest {

  @Test
  void blankEnvironmentIsAbsent() {
    EnvironmentContext context = EnvironmentContext.of(" ", "16.0");

    assertThat(context.hasEnvironmentKind()).isFalse();
    assertThat(context.normalizedKind()).isEmpty();
    assertThat(context.normalizedVersion()).isEmpty();
    assertThat(context.environmentVersion()).isEmpty();
  }

  @Test
  void normalizedEnvironmentKindIsTrimmedAndLowercase() {
    EnvironmentContext context = EnvironmentContext.of(" FloeDB ", "16.0");

    assertThat(context.hasEnvironmentKind()).isTrue();
    assertThat(context.normalizedKind()).isEqualTo("floedb");
    assertThat(context.environmentVersion()).isEqualTo("16.0");
  }

  @Test
  void readsEnvironmentHeaders() {
    EnvironmentContext context =
        EnvironmentContext.fromHeaders(
            name ->
                switch (name) {
                  case EnvironmentContext.HEADER_KIND -> " Floe ";
                  case EnvironmentContext.HEADER_VERSION -> "1.2";
                  default -> null;
                });

    assertThat(context.environmentKind()).isEqualTo("Floe");
    assertThat(context.environmentVersion()).isEqualTo("1.2");
    assertThat(context.normalizedKind()).isEqualTo("floe");
  }
}

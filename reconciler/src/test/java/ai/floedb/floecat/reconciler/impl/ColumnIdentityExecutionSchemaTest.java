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

package ai.floedb.floecat.reconciler.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMode;
import org.junit.jupiter.api.Test;

class ColumnIdentityExecutionSchemaTest {
  @Test
  void roundTripsIdentityAndChangesExecutionSchemaSignature() {
    ColumnIdentityMap identityMap = identityMap("sha256:identity");

    String attached = ColumnIdentityExecutionSchema.attach("{\"type\":\"struct\"}", identityMap);

    assertThat(attached).isNotEqualTo("{\"type\":\"struct\"}");
    assertThat(ColumnIdentityExecutionSchema.identityMap(attached)).isEqualTo(identityMap);
  }

  @Test
  void rejectsMismatchedExecutionFingerprint() {
    String attached =
        ColumnIdentityExecutionSchema.attach(
            "{\"type\":\"struct\"}", identityMap("sha256:identity"));
    String tampered = attached.replace("sha256:identity", "sha256:different");

    assertThatThrownBy(() -> ColumnIdentityExecutionSchema.identityMap(tampered))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("fingerprint mismatch");
  }

  @Test
  void rejectsMissingOrNonObjectExecutionSchema() {
    ColumnIdentityMap identityMap = identityMap("sha256:identity");

    assertThatThrownBy(() -> ColumnIdentityExecutionSchema.attach("", identityMap))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("JSON object");
    assertThatThrownBy(() -> ColumnIdentityExecutionSchema.attach("[]", identityMap))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("JSON object");
  }

  private static ColumnIdentityMap identityMap(String fingerprint) {
    return ColumnIdentityMap.newBuilder()
        .setFormatVersion(1)
        .setSourceVersion(1L)
        .setHighWaterMark(1L)
        .setMode(ColumnIdentityMode.COLUMN_IDENTITY_MODE_STRUCTURED_PATH)
        .setFingerprint(fingerprint)
        .build();
  }
}

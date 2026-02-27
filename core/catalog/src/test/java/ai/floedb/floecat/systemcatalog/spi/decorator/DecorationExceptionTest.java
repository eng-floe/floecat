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

package ai.floedb.floecat.systemcatalog.spi.decorator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.query.rpc.ColumnFailureCode;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DecorationExceptionTest {

  @Test
  void coreConstructorRejectsNullOrUnspecifiedCode() {
    assertThatThrownBy(() -> new DecorationException((ColumnFailureCode) null, "x"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("core code");

    assertThatThrownBy(
            () -> new DecorationException(ColumnFailureCode.COLUMN_FAILURE_CODE_UNSPECIFIED, "x"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("core code");
  }

  @Test
  void extensionConstructorRejectsNonPositiveCode() {
    assertThatThrownBy(() -> new DecorationException(0, "x"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("positive extension code");

    assertThatThrownBy(() -> new DecorationException(-7, "x"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("positive extension code");
  }

  @Test
  void coreConstructorStoresCoreCodeAndDefensiveCopiesDetails() {
    Map<String, String> details = new HashMap<>();
    details.put("key", "value");

    DecorationException exception =
        new DecorationException(
            ColumnFailureCode.COLUMN_FAILURE_CODE_TYPE_NOT_SUPPORTED,
            "type not supported",
            null,
            details);

    details.put("key", "mutated");

    assertThat(exception.code())
        .isEqualTo(ColumnFailureCode.COLUMN_FAILURE_CODE_TYPE_NOT_SUPPORTED);
    assertThat(exception.hasExtensionCodeValue()).isFalse();
    assertThat(exception.extensionCodeValue()).isNull();
    assertThat(exception.details()).containsEntry("key", "value");
    assertThatThrownBy(() -> exception.details().put("x", "y"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void extensionConstructorStoresExtensionCodeAndDefensiveCopiesDetails() {
    Map<String, String> details = new HashMap<>();
    details.put("floe_code", "7");

    DecorationException exception =
        new DecorationException(1207, "extension failed", null, details);

    details.put("floe_code", "99");

    assertThat(exception.code()).isEqualTo(ColumnFailureCode.COLUMN_FAILURE_CODE_ENGINE_EXTENSION);
    assertThat(exception.hasExtensionCodeValue()).isTrue();
    assertThat(exception.extensionCodeValue()).isEqualTo(1207);
    assertThat(exception.details()).containsEntry("floe_code", "7");
    assertThatThrownBy(() -> exception.details().put("x", "y"))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}

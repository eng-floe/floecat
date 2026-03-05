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

package ai.floedb.floecat.types;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class Int64CoercionsTest {

  @Test
  void checkedLongAcceptsLongBounds() {
    assertThat(Int64Coercions.checkedLong(Long.MIN_VALUE)).isEqualTo(Long.MIN_VALUE);
    assertThat(Int64Coercions.checkedLong(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void checkedLongRejectsOverflow() {
    BigInteger tooLarge = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
    assertThatThrownBy(() -> Int64Coercions.checkedLong(tooLarge))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("out of 64-bit range");
  }

  @Test
  void checkedLongRejectsFractionalBigDecimal() {
    assertThatThrownBy(() -> Int64Coercions.checkedLong(new BigDecimal("1.5")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("whole 64-bit");
  }
}

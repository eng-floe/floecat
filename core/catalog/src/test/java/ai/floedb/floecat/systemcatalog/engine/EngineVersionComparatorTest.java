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

package ai.floedb.floecat.systemcatalog.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EngineVersionComparatorTest {

  @Test
  void nullOrBlankNormalizesToZero() {
    assertThat(EngineVersionComparator.compare(null, "0")).isZero();
    assertThat(EngineVersionComparator.compare("", "0")).isZero();
    assertThat(EngineVersionComparator.compare("  ", "0")).isZero();
  }

  @Test
  void releaseIsGreaterThanPrerelease() {
    assertThat(EngineVersionComparator.compare("1.0", "1.0-beta")).isPositive();
  }

  @Test
  void hyphenatedAndConcatenatedSuffixTreatSame() {
    assertThat(EngineVersionComparator.compare("1.0rc1", "1.0-rc1")).isZero();
  }

  @Test
  void numericPrereleaseHasLowerPrecedenceThanAlpha() {
    assertThat(EngineVersionComparator.compare("1.0-1", "1.0-alpha")).isNegative();
  }

  @Test
  void leadingZerosInCoreSegmentsAreIgnored() {
    assertThat(EngineVersionComparator.compare("2024.01.15-alpha", "2024.1.15-alpha")).isZero();
  }

  @Test
  void prereleaseTokenOrderingHandlesLongForms() {
    assertThat(EngineVersionComparator.compare("1.0-alpha.1", "1.0-alpha.2")).isNegative();
    assertThat(EngineVersionComparator.compare("1.0-alpha", "1.0-alpha.0")).isNegative();
    assertThat(EngineVersionComparator.compare("1.0-rc1", "1.0-rc2")).isNegative();
    assertThat(EngineVersionComparator.compare("1.0-rc10", "1.0-rc2")).isNegative();
  }
}

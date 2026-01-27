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

import java.util.List;
import org.junit.jupiter.api.Test;

class VersionIntervalsTest {

  @Test
  void overlapsRespectsInclusiveBounds() {
    var left = interval("1.0", "2.0");
    var right = interval("2.0", "3.0");

    assertThat(VersionIntervals.overlaps(left, right)).isTrue();
    assertThat(VersionIntervals.overlaps(left, interval("2.1", "3.0"))).isFalse();
    assertThat(VersionIntervals.overlaps(left, interval("1.5", "1.9"))).isTrue();
  }

  @Test
  void unionMergesOverlappingAndTouchingIntervals() {
    var merged =
        VersionIntervals.union(
            List.of(interval("1.0", "2.0"), interval("2.0", "3.5"), interval("4.0", "5.0")));

    assertThat(merged).containsExactly(interval("1.0", "3.5"), interval("4.0", "5.0"));
  }

  @Test
  void coversValidatesTargetIsWithinUnion() {
    var union = VersionIntervals.union(List.of(interval("1.0", "2.0"), interval("3.0", "4.0")));

    assertThat(VersionIntervals.covers(union, interval("1.1", "1.9"))).isTrue();
    assertThat(VersionIntervals.covers(union, interval("2.5", "3.5"))).isFalse();
    assertThat(VersionIntervals.covers(union, interval("1.0", "4.0"))).isFalse();

    var contiguousUnion =
        VersionIntervals.union(List.of(interval("1.0", "2.5"), interval("2.5", "4.0")));
    assertThat(VersionIntervals.covers(contiguousUnion, interval("1.0", "4.0"))).isTrue();
  }

  @Test
  void coversFailsWhenGapBetweenIntervals() {
    var coverage = VersionIntervals.union(List.of(interval("1.0", "2.0"), interval("3.0", "4.0")));
    assertThat(VersionIntervals.covers(coverage, interval("1.0", "4.0"))).isFalse();
  }

  @Test
  void coversSucceedsWhenIntervalsTouch() {
    var coverage = VersionIntervals.union(List.of(interval("1.0", "2.0"), interval("2.0", "4.0")));
    assertThat(VersionIntervals.covers(coverage, interval("1.0", "4.0"))).isTrue();
  }

  @Test
  void intervalsHandleInfiniteBounds() {
    var left =
        new VersionIntervals.VersionInterval(
            VersionIntervals.VersionBound.negativeInfinity(), bound("1.0"));
    var right =
        new VersionIntervals.VersionInterval(
            bound("0.5"), VersionIntervals.VersionBound.positiveInfinity());
    assertThat(VersionIntervals.overlaps(left, right)).isTrue();

    var union =
        VersionIntervals.union(
            List.of(
                left,
                new VersionIntervals.VersionInterval(
                    bound("2.0"), VersionIntervals.VersionBound.positiveInfinity())));
    assertThat(union)
        .contains(
            new VersionIntervals.VersionInterval(
                VersionIntervals.VersionBound.negativeInfinity(), bound("1.0")));
    assertThat(
            VersionIntervals.covers(
                union,
                new VersionIntervals.VersionInterval(
                    bound("2.5"), VersionIntervals.VersionBound.positiveInfinity())))
        .isTrue();
  }

  private static VersionIntervals.VersionInterval interval(String min, String max) {
    return new VersionIntervals.VersionInterval(bound(min), bound(max));
  }

  private static VersionIntervals.VersionBound bound(String value) {
    return VersionIntervals.VersionBound.finite(EngineVersionComparator.normalize(value));
  }
}

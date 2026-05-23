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

package ai.floedb.floecat.reconciler.jobs;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

class ReconcileExecutionPolicyTest {

  // ---- defaults() ---------------------------------------------------------

  @Test
  void defaultsReturnsSingletonInstance() {
    assertSame(ReconcileExecutionPolicy.defaults(), ReconcileExecutionPolicy.defaults());
  }

  @Test
  void defaultsHasP3BackgroundAndScoreZero() {
    ReconcileExecutionPolicy p = ReconcileExecutionPolicy.defaults();
    assertEquals(StatsPriorityClass.P3_BACKGROUND, p.priorityClass());
    assertEquals(0L, p.priorityScore());
    assertEquals(ReconcileExecutionClass.DEFAULT, p.executionClass());
    assertTrue(p.lane().isBlank());
    assertTrue(p.attributes().isEmpty());
  }

  @Test
  void defaultsIsDefaultPolicy() {
    assertTrue(ReconcileExecutionPolicy.defaults().isDefaultPolicy());
  }

  // ---- legacy of(ReconcileExecutionClass, ...) ----------------------------

  @Test
  void legacyOfWithAllDefaultsReturnsSingleton() {
    ReconcileExecutionPolicy p =
        ReconcileExecutionPolicy.of(ReconcileExecutionClass.DEFAULT, null, null);
    assertSame(ReconcileExecutionPolicy.defaults(), p);
  }

  @Test
  void legacyOfWithNonDefaultClassSetsP3BackgroundAndScoreZero() {
    ReconcileExecutionPolicy p =
        ReconcileExecutionPolicy.of(ReconcileExecutionClass.INTERACTIVE, "lane1", Map.of("k", "v"));
    assertEquals(ReconcileExecutionClass.INTERACTIVE, p.executionClass());
    assertEquals(StatsPriorityClass.P3_BACKGROUND, p.priorityClass());
    assertEquals(0L, p.priorityScore());
    assertEquals("lane1", p.lane());
    assertEquals(Map.of("k", "v"), p.attributes());
  }

  @Test
  void legacyOfIsNotDefaultPolicy_whenExecutionClassNonDefault() {
    ReconcileExecutionPolicy p =
        ReconcileExecutionPolicy.of(ReconcileExecutionClass.INTERACTIVE, null, null);
    assertFalse(p.isDefaultPolicy());
  }

  // ---- of(StatsPriorityClass, String, Map, long) --------------------------

  @Test
  void fullOfFactorySetsPriorityClassAndScore() {
    ReconcileExecutionPolicy p =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P0_SYNC,
            "acct1:tbl1",
            Map.of("enqueue_reason", "sync_capture"),
            42L);
    assertEquals(StatsPriorityClass.P0_SYNC, p.priorityClass());
    assertEquals(42L, p.priorityScore());
    assertEquals("acct1:tbl1", p.lane());
    assertEquals("sync_capture", p.attributes().get("enqueue_reason"));
    assertFalse(p.isDefaultPolicy());
  }

  @Test
  void fullOfFactoryWithP3AndScoreZeroAndNoLaneReturnsSingleton() {
    ReconcileExecutionPolicy p =
        ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, null, null, 0L);
    assertSame(ReconcileExecutionPolicy.defaults(), p);
  }

  @Test
  void fullOfFactoryClampsPriorityScoreToZero() {
    ReconcileExecutionPolicy p =
        ReconcileExecutionPolicy.of(StatsPriorityClass.P1_FRESHNESS, "lane", null, -50L);
    assertEquals(0L, p.priorityScore());
  }

  // ---- of(StatsPriorityClass, String, Map) convenience -------------------

  @Test
  void convenienceOfSetsScoreZero() {
    ReconcileExecutionPolicy p =
        ReconcileExecutionPolicy.of(StatsPriorityClass.P2_REPAIR, "lane", null);
    assertEquals(StatsPriorityClass.P2_REPAIR, p.priorityClass());
    assertEquals(0L, p.priorityScore());
  }

  @Test
  void convenienceOfP3NullNullReturnsSingleton() {
    assertSame(
        ReconcileExecutionPolicy.defaults(),
        ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, null, null));
  }

  // ---- isDefaultPolicy() --------------------------------------------------

  @Test
  void isDefaultPolicyFalseWhenPriorityClassDiffers() {
    ReconcileExecutionPolicy p =
        ReconcileExecutionPolicy.of(StatsPriorityClass.P1_FRESHNESS, null, null, 0L);
    assertFalse(p.isDefaultPolicy());
  }

  @Test
  void isDefaultPolicyFalseWhenScoreDiffers() {
    ReconcileExecutionPolicy p =
        ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, null, null, 1L);
    assertFalse(p.isDefaultPolicy());
  }

  @Test
  void isDefaultPolicyFalseWhenLaneDiffers() {
    ReconcileExecutionPolicy p =
        ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, "some-lane", null, 0L);
    assertFalse(p.isDefaultPolicy());
  }

  @Test
  void isDefaultPolicyFalseWhenAttributesDiffer() {
    ReconcileExecutionPolicy p =
        ReconcileExecutionPolicy.of(StatsPriorityClass.P3_BACKGROUND, null, Map.of("k", "v"), 0L);
    assertFalse(p.isDefaultPolicy());
  }

  // ---- compact constructor normalization ----------------------------------

  @Test
  void attributesAreNormalizedAndImmutable() {
    ReconcileExecutionPolicy p =
        ReconcileExecutionPolicy.of(
            StatsPriorityClass.P1_FRESHNESS,
            " lane ",
            Map.of("  key  ", "  value  ", "", "ignored"),
            10L);
    assertEquals("lane", p.lane());
    assertEquals("value", p.attributes().get("key"));
    assertFalse(p.attributes().containsKey(""));
    assertFalse(p.attributes().containsKey("  key  "));
    assertThrows(UnsupportedOperationException.class, () -> p.attributes().put("x", "y"));
  }

  @Test
  void nullAttributeValueNormalizesToEmptyString() {
    Map<String, String> attrs = new java.util.HashMap<>();
    attrs.put("k", null);
    ReconcileExecutionPolicy p =
        ReconcileExecutionPolicy.of(StatsPriorityClass.P1_FRESHNESS, "lane", attrs, 0L);
    assertEquals("", p.attributes().get("k"));
  }
}

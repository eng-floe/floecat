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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class ReconcileExecutionPolicyTest {

  @Test
  void defaultsReturnsCachedSingleton() {
    assertSame(ReconcileExecutionPolicy.defaults(), ReconcileExecutionPolicy.defaults());
  }

  @Test
  void defaultsHasP3Background() {
    assertEquals(
        StatsPriorityClass.P3_BACKGROUND, ReconcileExecutionPolicy.defaults().priorityClass());
    assertEquals(0L, ReconcileExecutionPolicy.defaults().priorityScore());
  }

  @Test
  void legacyThreeArgFactoryDefaultsToP3AndScoreZero() {
    var policy = ReconcileExecutionPolicy.of(ReconcileExecutionClass.DEFAULT, "my-lane", Map.of());
    assertEquals(StatsPriorityClass.P3_BACKGROUND, policy.priorityClass());
    assertEquals(0L, policy.priorityScore());
    assertEquals("my-lane", policy.lane());
  }

  @Test
  void fiveArgFactoryPreservesPriorityClass() {
    var policy =
        ReconcileExecutionPolicy.of(
            ReconcileExecutionClass.DEFAULT,
            "lane",
            Map.of(),
            StatsPriorityClass.P1_FRESHNESS,
            42L);
    assertEquals(StatsPriorityClass.P1_FRESHNESS, policy.priorityClass());
    assertEquals(42L, policy.priorityScore());
  }

  @Test
  void priorityOnlyFactoryUsesDefaultExecutionClass() {
    var policy = ReconcileExecutionPolicy.of(StatsPriorityClass.P2_REPAIR, "lane", Map.of(), 99L);
    assertEquals(StatsPriorityClass.P2_REPAIR, policy.priorityClass());
    assertEquals(ReconcileExecutionClass.DEFAULT, policy.executionClass());
    assertEquals(99L, policy.priorityScore());
  }

  @Test
  void negativeScoreClampedToZero() {
    var policy =
        ReconcileExecutionPolicy.of(
            ReconcileExecutionClass.DEFAULT, "", Map.of(), StatsPriorityClass.P3_BACKGROUND, -5L);
    assertEquals(0L, policy.priorityScore());
  }

  @Test
  void isDefaultPolicyOnlyForAllDefaultFields() {
    assertTrue(ReconcileExecutionPolicy.defaults().isDefaultPolicy());
    assertFalse(
        ReconcileExecutionPolicy.of(
                ReconcileExecutionClass.DEFAULT, "", Map.of(), StatsPriorityClass.P1_FRESHNESS, 0L)
            .isDefaultPolicy());
    assertFalse(
        ReconcileExecutionPolicy.of(
                ReconcileExecutionClass.DEFAULT, "", Map.of(), StatsPriorityClass.P3_BACKGROUND, 1L)
            .isDefaultPolicy());
  }

  @Test
  void allDefaultFieldsReturnCachedSingleton() {
    var policy = ReconcileExecutionPolicy.of(ReconcileExecutionClass.DEFAULT, null, null, null, 0L);
    assertSame(ReconcileExecutionPolicy.defaults(), policy);
  }
}

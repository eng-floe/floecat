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

package ai.floedb.floecat.service.reconciler.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class AccountTagFilterTest {

  private static final Map<String, String> UNTAGGED = Map.of();
  private static final Map<String, String> THIS_RUN = Map.of("ci-run", "run-1");
  private static final Map<String, String> OTHER_RUN = Map.of("ci-run", "run-2");

  // ----- ignore-by-key (steady-state floecat) -----

  @Test
  void ignoreKeySkipsAnyAccountCarryingTheKeyRegardlessOfValue() {
    var filter = new AccountTagFilter.IgnoreKey("ci-run");
    assertFalse(filter.accountPasses(THIS_RUN));
    assertFalse(filter.accountPasses(OTHER_RUN));
  }

  @Test
  void ignoreKeyOwnsUntaggedAccounts() {
    var filter = new AccountTagFilter.IgnoreKey("ci-run");
    assertTrue(filter.accountPasses(UNTAGGED));
    assertTrue(filter.accountPasses(Map.of("other-key", "v")));
  }

  // ----- own-by-exact-value (overlay floecat) -----

  @Test
  void ownValueManagesOnlyTheAccountWithTheExactValue() {
    var filter = new AccountTagFilter.OwnValue("ci-run", "run-1");
    assertTrue(filter.accountPasses(THIS_RUN));
  }

  @Test
  void ownValueRejectsAnotherRunsAccount() {
    var filter = new AccountTagFilter.OwnValue("ci-run", "run-1");
    assertFalse(filter.accountPasses(OTHER_RUN));
  }

  @Test
  void ownValueIgnoresUntaggedAccounts() {
    var filter = new AccountTagFilter.OwnValue("ci-run", "run-1");
    assertFalse(filter.accountPasses(UNTAGGED));
  }

  // ----- pass-all (no filter configured) -----

  @Test
  void passAllManagesEveryAccount() {
    var filter = new AccountTagFilter.PassAll();
    assertTrue(filter.accountPasses(UNTAGGED));
    assertTrue(filter.accountPasses(THIS_RUN));
    assertTrue(filter.accountPasses(OTHER_RUN));
  }

  // ----- parse -----

  @Test
  void parseBlankOrNullYieldsPassAll() {
    assertEquals(new AccountTagFilter.PassAll(), AccountTagFilter.parse(""));
    assertEquals(new AccountTagFilter.PassAll(), AccountTagFilter.parse("   "));
    assertEquals(new AccountTagFilter.PassAll(), AccountTagFilter.parse(null));
  }

  @Test
  void parseIgnoreKey() {
    assertEquals(
        new AccountTagFilter.IgnoreKey("ci-run"), AccountTagFilter.parse("ignore-key:ci-run"));
    assertEquals(
        new AccountTagFilter.IgnoreKey("ci-run"), AccountTagFilter.parse("  ignore-key:ci-run  "));
  }

  @Test
  void parseOwnValue() {
    assertEquals(
        new AccountTagFilter.OwnValue("ci-run", "run-1"),
        AccountTagFilter.parse("own-value:ci-run=run-1"));
  }

  @Test
  void parseRejectsMalformedSpecs() {
    assertThrows(IllegalArgumentException.class, () -> AccountTagFilter.parse("nonsense"));
    assertThrows(IllegalArgumentException.class, () -> AccountTagFilter.parse("ignore-key:"));
    assertThrows(IllegalArgumentException.class, () -> AccountTagFilter.parse("own-value:ci-run"));
    assertThrows(IllegalArgumentException.class, () -> AccountTagFilter.parse("own-value:=run-1"));
  }
}

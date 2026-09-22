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

package ai.floedb.floecat.service.repo.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

class KeysPlannerFamilyTest {
  private static final String ACCOUNT = "acct";

  /**
   * The invariant the index rests on. Absence in a complete partition is authoritative, so a key
   * the index admits must sit under a prefix its load actually reads; a planner family outside the
   * load prefixes would report rows that exist as missing. Driven by reflection so a key family
   * added later cannot quietly sit outside it.
   */
  @Test
  void everyPlannerKeyIsCoveredByALoadPrefix() {
    List<String> prefixes = Keys.plannerFamilyPrefixes(ACCOUNT);
    String accountRoot = Keys.accountRootPrefix(ACCOUNT);
    // The load reads the prefixes, then the per-table keys no prefix reaches. Both phases count.
    List<String> perTable = Keys.plannerTableKeys(ACCOUNT, ACCOUNT);
    List<String> uncovered = new ArrayList<>();

    for (String key : everyAccountScopedKey()) {
      if (Keys.pointerNamespace(key) != Keys.PointerNamespace.PLANNER || key.equals(accountRoot)) {
        continue;
      }
      if (prefixes.stream().noneMatch(key::startsWith) && !perTable.contains(key)) {
        uncovered.add(key);
      }
    }

    assertThat(uncovered).as("planner keys that no load prefix reads").isEmpty();
  }

  /**
   * Pinned on purpose. The coverage test above derives both sides from this one list, so it cannot
   * notice the list shrinking -- dropping a family would simply move it to durable and stay green.
   * This is the assertion that makes removing one a deliberate act.
   */
  @Test
  void theAllowlistHoldsExactlyTheKnownPlannerFamilies() {
    String root = Keys.accountRootPrefix(ACCOUNT);
    assertThat(Keys.plannerFamilyPrefixes(ACCOUNT))
        .containsExactly(
            root + "account/",
            root + "catalog-integrations/",
            root + "catalog-overlays/",
            root + "catalogs/",
            root + "connectors/",
            root + "namespaces/",
            root + "relations/",
            root + "storage-authorities/",
            root + "tables/by-id/",
            root + "views/");
  }

  /**
   * A misspelled family would be a no-op that nothing else notices: no key matches it, so the whole
   * family quietly leaves the index and is answered from durable KV instead.
   */
  @Test
  void everyAllowlistedFamilyIsOneKeysActuallyBuilds() {
    List<String> keys = everyAccountScopedKey();
    assertThat(Keys.plannerFamilyPrefixes(ACCOUNT))
        .allSatisfy(
            prefix ->
                assertThat(keys)
                    .as("no key Keys builds lives under %s", prefix)
                    .anySatisfy(key -> assertThat(key).startsWith(prefix)));
  }

  /**
   * The coverage guarantee is only as good as this enumeration, so its blind spots are pinned. A
   * builder reflection cannot call is a builder these tests do not check -- adding one has to be a
   * decision, not a silent gap.
   */
  @Test
  void reflectionReachesEveryKeyBuilderButTheKnownExceptions() {
    assertThat(unreachableKeyBuilders())
        .isEqualTo(
            // Derives its result from a pointer key it is handed, so it cannot introduce a family.
            Set.of("idempotencyBlobPrefixForPointerKey"));
  }

  @Test
  void theLoadPrefixesAreNarrowerThanTheAccountRoot() {
    // Scanning the account root is what read every operational row only to discard it.
    assertThat(Keys.plannerFamilyPrefixes(ACCOUNT))
        .isNotEmpty()
        .doesNotContain(Keys.accountRootPrefix(ACCOUNT))
        .allSatisfy(prefix -> assertThat(prefix).startsWith(Keys.accountRootPrefix(ACCOUNT)));
  }

  /**
   * A table commits far faster than its schema changes, so everything keyed by snapshot grows with
   * ingest. At a five-second commit cadence those rows outrun any heap; only identity and the two
   * current pointers stay resident.
   */
  @Test
  void onlyTableIdentityAndCurrentPointersAreResident() {
    assertThat(Keys.pointerNamespace(Keys.tablePointerById(ACCOUNT, "t")))
        .isEqualTo(Keys.PointerNamespace.PLANNER);
    assertThat(Keys.pointerNamespace(Keys.tableRootByTable(ACCOUNT, "t")))
        .isEqualTo(Keys.PointerNamespace.PLANNER);
    assertThat(Keys.pointerNamespace(Keys.currentSnapshotPointerByTable(ACCOUNT, "t")))
        .isEqualTo(Keys.PointerNamespace.PLANNER);

    for (String perSnapshot :
        List.of(
            Keys.snapshotPointerById(ACCOUNT, "t", 1L),
            Keys.snapshotPointerByTime(ACCOUNT, "t", 1L, 1L),
            Keys.snapshotConstraintsPointer(ACCOUNT, "t", 1L),
            Keys.snapshotTargetStatsManifestPointer(ACCOUNT, "t", 1L),
            Keys.snapshotTargetStatsGenerationPointer(ACCOUNT, "t", 1L, "g", "col"),
            Keys.snapshotIndexArtifactActiveGenerationPointer(ACCOUNT, "t", 1L),
            Keys.snapshotIndexArtifactGenerationPointer(ACCOUNT, "t", 1L, "g", "a"))) {
      assertThat(Keys.pointerNamespace(perSnapshot))
          .as(perSnapshot)
          .isEqualTo(Keys.PointerNamespace.OPERATIONAL);
    }
  }

  /** The classifier reads a suffix list; the loader calls the key builders. Pin that they agree. */
  @Test
  void theBuiltPerTableKeysAreExactlyTheOnesTheClassifierAdmits() {
    for (String key : Keys.plannerTableKeys(ACCOUNT, "tbl")) {
      assertThat(Keys.pointerNamespace(key)).as(key).isEqualTo(Keys.PointerNamespace.PLANNER);
      assertThat(key).startsWith(Keys.accountRootPrefix(ACCOUNT) + "tables/tbl/");
    }
    assertThat(Keys.plannerTableKeys(ACCOUNT, "tbl"))
        .containsExactlyInAnyOrder(
            Keys.accountRootPrefix(ACCOUNT) + "tables/tbl/root/current",
            Keys.accountRootPrefix(ACCOUNT) + "tables/tbl/snapshots/current");
  }

  @Test
  void anUnclassifiedFamilyStaysOnDurableStorage() {
    // The whole point of the allowlist: a family nobody classified must not become resident heap.
    assertThat(Keys.pointerNamespace(Keys.accountRootPrefix(ACCOUNT) + "brand-new-family/thing"))
        .isEqualTo(Keys.PointerNamespace.OPERATIONAL);
  }

  @Test
  void theFamiliesThatWereOperationalStillAre() {
    for (String family :
        List.of("transactions", "idempotency", "reconcile", "gc", "root-resyncs")) {
      assertThat(Keys.pointerNamespace(Keys.accountRootPrefix(ACCOUNT) + family + "/x/y"))
          .as(family)
          .isEqualTo(Keys.PointerNamespace.OPERATIONAL);
    }
  }

  @Test
  void theFamiliesThatWerePlannerStillAre() {
    assertThat(Keys.pointerNamespace(Keys.tablePointerById(ACCOUNT, "t")))
        .isEqualTo(Keys.PointerNamespace.PLANNER);
    assertThat(Keys.pointerNamespace(Keys.catalogPointerById(ACCOUNT, "c")))
        .isEqualTo(Keys.PointerNamespace.PLANNER);
    assertThat(Keys.pointerNamespace(Keys.accountRootPrefix(ACCOUNT)))
        .as("the account root prefix covers the planner subtree")
        .isEqualTo(Keys.PointerNamespace.PLANNER);
    assertThat(Keys.pointerNamespace(Keys.tablePointerByIdPrefix(ACCOUNT)))
        .as("the table identity listing is how SHOW TABLES is served")
        .isEqualTo(Keys.PointerNamespace.PLANNER);
  }

  @Test
  void deletionMarkersKeepTheirOldMeaning() {
    // The account's own marker is operational; a resource *named* deleting is planner state.
    assertThat(Keys.pointerNamespace(Keys.accountDeletionMarker(ACCOUNT)))
        .isEqualTo(Keys.PointerNamespace.OPERATIONAL);
    assertThat(Keys.pointerNamespace(Keys.catalogIntegrationDeletionMarker(ACCOUNT, "i")))
        .isEqualTo(Keys.PointerNamespace.PLANNER);
  }

  @Test
  void markersInsideAPlannerFamilyStayOperational() {
    assertThat(Keys.pointerNamespace(Keys.namespaceChildrenMarker(ACCOUNT, "ns")))
        .isEqualTo(Keys.PointerNamespace.OPERATIONAL);
  }

  private static Set<String> unreachableKeyBuilders() {
    TreeSet<String> unreachable = new TreeSet<>();
    collect(unreachable);
    return unreachable;
  }

  /** Every account-scoped string Keys can build, so a new family cannot dodge these assertions. */
  private static List<String> everyAccountScopedKey() {
    return collect(new TreeSet<>());
  }

  private static List<String> collect(TreeSet<String> unreachableOut) {
    TreeSet<String> keys = new TreeSet<>();
    for (Method method : Keys.class.getDeclaredMethods()) {
      if (!Modifier.isStatic(method.getModifiers())
          || !Modifier.isPublic(method.getModifiers())
          || method.getReturnType() != String.class) {
        continue;
      }
      Object[] args = new Object[method.getParameterCount()];
      boolean buildable = true;
      for (int i = 0; i < args.length && buildable; i++) {
        args[i] = sample(method.getParameterTypes()[i]);
        buildable = args[i] != UNSUPPORTED;
      }
      if (!buildable) {
        unreachableOut.add(method.getName());
        continue;
      }
      try {
        String produced = (String) method.invoke(null, args);
        if (produced != null && produced.startsWith(Keys.accountRootPrefix())) {
          keys.add(produced);
        }
      } catch (ReflectiveOperationException | RuntimeException rejectedArguments) {
        unreachableOut.add(method.getName());
      }
    }
    assertThat(keys).as("reflection reached the key builders").hasSizeGreaterThan(100);
    return List.copyOf(keys);
  }

  private static final Object UNSUPPORTED = new Object();

  private static Object sample(Class<?> type) {
    if (type == String.class) {
      return ACCOUNT;
    }
    if (type == long.class || type == Long.class) {
      return 1L;
    }
    if (type == int.class || type == Integer.class) {
      return 1;
    }
    if (type == boolean.class || type == Boolean.class) {
      return Boolean.FALSE;
    }
    if (type == Optional.class) {
      return Optional.empty();
    }
    if (type == List.class) {
      return List.of("seg");
    }
    if (type == byte[].class) {
      return new byte[32];
    }
    if (type.isEnum()) {
      Object[] constants = type.getEnumConstants();
      return constants.length > 0 ? constants[0] : UNSUPPORTED;
    }
    return UNSUPPORTED;
  }
}

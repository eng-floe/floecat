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

package ai.floedb.floecat.service.repo.cache;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.service.repo.model.Keys;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

class PlannerPointerShapeTest {
  private static final String ACCOUNT = "acct";

  @Test
  void everyResidentKeyIsCoveredByALoadPath() {
    List<String> prefixes = PlannerPointerShape.loadPrefixes(ACCOUNT);
    List<String> perTable = PlannerPointerShape.perTableKeys(ACCOUNT, ACCOUNT);
    List<String> uncovered = new ArrayList<>();

    for (String key : everyAccountScopedKey()) {
      if (!PlannerPointerShape.isResidentKey(key)) {
        continue;
      }
      if (prefixes.stream().noneMatch(key::startsWith) && !perTable.contains(key)) {
        uncovered.add(key);
      }
    }

    assertThat(uncovered).as("resident keys that no load path reads").isEmpty();
  }

  @Test
  void theLoadPrefixesHoldExactlyTheKnownResidentFamilies() {
    String root = Keys.accountRootPrefix(ACCOUNT);
    assertThat(PlannerPointerShape.loadPrefixes(ACCOUNT))
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

  @Test
  void everyLoadPrefixIsOneKeysActuallyBuilds() {
    List<String> keys = everyAccountScopedKey();
    assertThat(PlannerPointerShape.loadPrefixes(ACCOUNT))
        .allSatisfy(
            prefix ->
                assertThat(keys)
                    .as("no key Keys builds lives under %s", prefix)
                    .anySatisfy(key -> assertThat(key).startsWith(prefix)));
  }

  @Test
  void reflectionReachesEveryKeyBuilderButTheKnownExceptions() {
    assertThat(unreachableKeyBuilders())
        .isEqualTo(
            // Derives its result from a pointer key it is handed, so it cannot introduce a family.
            Set.of("idempotencyBlobPrefixForPointerKey"));
  }

  @Test
  void loadPrefixesAreNarrowerThanTheAccountRoot() {
    assertThat(PlannerPointerShape.loadPrefixes(ACCOUNT))
        .isNotEmpty()
        .doesNotContain(Keys.accountRootPrefix(ACCOUNT))
        .allSatisfy(prefix -> assertThat(prefix).startsWith(Keys.accountRootPrefix(ACCOUNT)));
  }

  @Test
  void onlyTableIdentityAndCurrentPointersAreResident() {
    assertThat(PlannerPointerShape.isResidentKey(Keys.tablePointerById(ACCOUNT, "t"))).isTrue();
    assertThat(PlannerPointerShape.isResidentKey(Keys.tablePointerById(ACCOUNT, "t") + "/future/x"))
        .isFalse();
    assertThat(PlannerPointerShape.isResidentKey(Keys.tableRootByTable(ACCOUNT, "t"))).isTrue();
    assertThat(PlannerPointerShape.isResidentKey(Keys.currentSnapshotPointerByTable(ACCOUNT, "t")))
        .isTrue();

    for (String perSnapshot :
        List.of(
            Keys.snapshotPointerById(ACCOUNT, "t", 1L),
            Keys.snapshotPointerByTime(ACCOUNT, "t", 1L, 1L),
            Keys.snapshotConstraintsPointer(ACCOUNT, "t", 1L),
            Keys.snapshotTargetStatsManifestPointer(ACCOUNT, "t", 1L),
            Keys.snapshotTargetStatsGenerationPointer(ACCOUNT, "t", 1L, "g", "col"),
            Keys.snapshotIndexArtifactActiveGenerationPointer(ACCOUNT, "t", 1L),
            Keys.snapshotIndexArtifactGenerationPointer(ACCOUNT, "t", 1L, "g", "a"))) {
      assertThat(PlannerPointerShape.isResidentKey(perSnapshot)).as(perSnapshot).isFalse();
    }
  }

  @Test
  void theBuiltPerTableKeysAreExactlyTheResidentTablePointers() {
    for (String key : PlannerPointerShape.perTableKeys(ACCOUNT, "tbl")) {
      assertThat(PlannerPointerShape.isResidentKey(key)).as(key).isTrue();
      assertThat(key).startsWith(Keys.accountRootPrefix(ACCOUNT) + "tables/tbl/");
    }
    assertThat(PlannerPointerShape.perTableKeys(ACCOUNT, "tbl"))
        .containsExactlyInAnyOrder(
            Keys.accountRootPrefix(ACCOUNT) + "tables/tbl/root/current",
            Keys.accountRootPrefix(ACCOUNT) + "tables/tbl/snapshots/current");
  }

  @Test
  void aResidentListPrefixHoldsOnlyResidentKeysBeneathIt() {
    List<String> all = everyAccountScopedKey();
    List<String> offenders = new ArrayList<>();
    for (String prefix : all) {
      if (!prefix.endsWith("/") || !PlannerPointerShape.isResidentListPrefix(prefix)) {
        continue;
      }
      for (String key : all) {
        if (key.equals(prefix) || !key.startsWith(prefix)) {
          continue;
        }
        if (!PlannerPointerShape.isResidentKey(key) && !Keys.isIdempotencyOrMarkerKey(key)) {
          offenders.add(prefix + "   serves   " + key);
        }
      }
    }
    assertThat(offenders).as("prefixes the index would answer incompletely").isEmpty();
  }

  @Test
  void anUnclassifiedFamilyStaysOnDurableStorage() {
    assertThat(
            PlannerPointerShape.isResidentKey(
                Keys.accountRootPrefix(ACCOUNT) + "brand-new-family/thing"))
        .isFalse();
  }

  @Test
  void operationalFamiliesStayDurableOnly() {
    for (String family :
        List.of("transactions", "idempotency", "reconcile", "gc", "root-resyncs")) {
      assertThat(
              PlannerPointerShape.isResidentKey(Keys.accountRootPrefix(ACCOUNT) + family + "/x/y"))
          .as(family)
          .isFalse();
    }
  }

  @Test
  void accountRootMutationsStillTouchResidentKeysButRootListingsDoNot() {
    assertThat(PlannerPointerShape.isResidentListPrefix(Keys.accountRootPrefix(ACCOUNT)))
        .as("the account root spans operational families")
        .isFalse();
    assertThat(
            PlannerPointerShape.mutationPrefixTouchesResidentKeys(Keys.accountRootPrefix(ACCOUNT)))
        .as("account-wide delete still has to be ordered against the index")
        .isTrue();
    assertThat(PlannerPointerShape.isResidentListPrefix(Keys.tablePointerByIdPrefix(ACCOUNT)))
        .as("the table identity listing is how SHOW TABLES is served")
        .isTrue();
  }

  @Test
  void snapshotAndStatsPrefixMutationsDoNotForceAResidentLoad() {
    assertThat(
            PlannerPointerShape.mutationPrefixTouchesResidentKeys(
                Keys.snapshotPointerByIdPrefix(ACCOUNT, "tbl")))
        .isFalse();
    assertThat(
            PlannerPointerShape.mutationPrefixTouchesResidentKeys(
                Keys.snapshotTargetStatsGenerationPrefix(ACCOUNT, "tbl", 7L, "gen")))
        .isFalse();
    assertThat(
            PlannerPointerShape.mutationPrefixTouchesResidentKeys(
                Keys.snapshotIndexArtifactGenerationPrefix(ACCOUNT, "tbl", 7L, "gen")))
        .isFalse();
    assertThat(
            PlannerPointerShape.mutationPrefixTouchesResidentKeys(
                Keys.tableBlobPrefix(ACCOUNT, "tbl")))
        .as("a whole-table delete does touch root/current and snapshots/current")
        .isTrue();
  }

  @Test
  void markersInsideAResidentFamilyStayDurableOnly() {
    assertThat(PlannerPointerShape.isResidentKey(Keys.namespaceChildrenMarker(ACCOUNT, "ns")))
        .isFalse();
  }

  private static Set<String> unreachableKeyBuilders() {
    TreeSet<String> unreachable = new TreeSet<>();
    collect(unreachable);
    return unreachable;
  }

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

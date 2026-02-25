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

package ai.floedb.floecat.systemcatalog.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.TableBackendKind;
import ai.floedb.floecat.systemcatalog.def.*;
import java.util.List;
import org.junit.jupiter.api.Test;

final class SystemEngineCatalogTest {

  private static NameRef name(String n) {
    return NameRef.newBuilder().setName(n).build();
  }

  // ---------------------------------------------------------------------------
  // Indexing behaviour
  // ---------------------------------------------------------------------------

  @Test
  void functionsByName_supportsOverloads() {
    SystemFunctionDef f1 =
        new SystemFunctionDef(
            name("add"), List.of(name("int")), name("int"), false, false, List.of());

    SystemFunctionDef f2 =
        new SystemFunctionDef(
            name("add"), List.of(name("double")), name("double"), false, false, List.of());

    SystemCatalogData data =
        new SystemCatalogData(
            List.of(f1, f2),
            List.of(),
            List.of(
                new SystemTypeDef(name("int"), "scalar", false, null, List.of()),
                new SystemTypeDef(name("double"), "scalar", false, null, List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    SystemEngineCatalog catalog = SystemEngineCatalog.from("spark", data);

    assertThat(catalog.functions("add")).hasSize(2).containsExactly(f1, f2);
  }

  // ---------------------------------------------------------------------------
  // indexUnique replacement semantics
  // ---------------------------------------------------------------------------

  @Test
  void indexUnique_replacesDuplicateByLast() {
    SystemTypeDef t1 = new SystemTypeDef(name("int"), "scalar", false, null, List.of());

    SystemTypeDef t2 = new SystemTypeDef(name("int"), "scalar_v2", false, null, List.of());

    SystemCatalogData data =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(t1, t2), // duplicate canonical name
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    SystemEngineCatalog catalog = SystemEngineCatalog.from("spark", data);

    assertThat(catalog.type("int")).isPresent().get().isEqualTo(t2); // last wins
  }

  // ---------------------------------------------------------------------------
  // Table lookup
  // ---------------------------------------------------------------------------

  @Test
  void table_lookupByCanonicalName() {
    SystemTableDef table =
        new SystemTableDef(
            name("orders"),
            "orders",
            List.<SystemColumnDef>of(),
            TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
            "scanner",
            "",
            "",
            List.of(),
            null);

    SystemCatalogData data =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(table),
            List.of(),
            List.of());

    SystemEngineCatalog catalog = SystemEngineCatalog.from("spark", data);

    assertThat(catalog.table("orders")).isPresent().get().isEqualTo(table);
  }

  // ---------------------------------------------------------------------------
  // Fingerprint behaviour
  // ---------------------------------------------------------------------------

  @Test
  void fingerprint_isStableForSameInput() {
    SystemCatalogData data =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(new SystemTypeDef(name("int"), "scalar", false, null, List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    String fp1 = SystemEngineCatalog.from("spark", data).fingerprint();
    String fp2 = SystemEngineCatalog.from("spark", data).fingerprint();

    assertThat(fp1).isEqualTo(fp2);
  }

  @Test
  void fingerprint_changesWhenCatalogChanges() {
    SystemCatalogData data1 =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(new SystemTypeDef(name("int"), "scalar", false, null, List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    SystemCatalogData data2 =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(
                new SystemTypeDef(name("int"), "scalar", false, null, List.of()),
                new SystemTypeDef(name("double"), "scalar", false, null, List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    String fp1 = SystemEngineCatalog.from("spark", data1).fingerprint();
    String fp2 = SystemEngineCatalog.from("spark", data2).fingerprint();

    assertThat(fp1).isNotEqualTo(fp2);
  }

  // ---------------------------------------------------------------------------
  // Immutability guarantees
  // ---------------------------------------------------------------------------

  @Test
  void exposedListsAreImmutable() {
    SystemCatalogData data =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(new SystemTypeDef(name("int"), "scalar", false, null, List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    SystemEngineCatalog catalog = SystemEngineCatalog.from("spark", data);

    assertThatThrownBy(
            () ->
                catalog.types().add(new SystemTypeDef(name("x"), "scalar", false, null, List.of())))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}

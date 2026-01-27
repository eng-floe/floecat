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

package ai.floedb.floecat.systemcatalog.provider;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import org.junit.jupiter.api.Test;

class FloecatInternalProviderTest {

  @Test
  void catalogDataContainsInformationSchemaTables() {
    SystemCatalogData catalog = FloecatInternalProvider.catalogData();

    assertThat(catalog.tables()).isNotEmpty();
    assertThat(catalog.tables().stream().map(table -> NameRefUtil.canonical(table.name())).toList())
        .contains(
            "information_schema.tables",
            "information_schema.columns",
            "information_schema.schemata");
  }
}

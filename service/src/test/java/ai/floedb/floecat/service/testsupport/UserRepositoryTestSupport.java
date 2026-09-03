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

package ai.floedb.floecat.service.testsupport;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;

/** Thin convenience adapters over the shared repository fakes used by resolver tests. */
public final class UserRepositoryTestSupport {

  private UserRepositoryTestSupport() {}

  public static final class FakeCatalogRepository
      extends ai.floedb.floecat.service.testsupport.FakeCatalogRepository {
    public void put(Catalog catalog) {
      super.put(catalog, null);
    }
  }

  public static final class FakeNamespaceRepository
      extends ai.floedb.floecat.service.testsupport.FakeNamespaceRepository {
    public void put(Namespace namespace) {
      super.put(namespace, null);
    }
  }

  public static final class FakeTableRepository
      extends ai.floedb.floecat.service.testsupport.FakeTableRepository {
    public void put(Table table) {
      super.put(table, null);
    }
  }

  public static final class FakeViewRepository
      extends ai.floedb.floecat.service.testsupport.FakeViewRepository {
    public void put(View view) {
      super.put(view, null);
    }
  }
}

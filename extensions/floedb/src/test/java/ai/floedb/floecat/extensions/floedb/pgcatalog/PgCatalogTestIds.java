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

package ai.floedb.floecat.extensions.floedb.pgcatalog;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;

final class PgCatalogTestIds {

  private static final String ENGINE = "floedb";
  private static final String PG_NAMESPACE = "pg_catalog";

  private PgCatalogTestIds() {}

  static ResourceId catalog() {
    return SystemNodeRegistry.systemCatalogContainerId(ENGINE);
  }

  static ResourceId namespace(String namespace) {
    return SystemNodeRegistry.resourceId(
        ENGINE, ResourceKind.RK_NAMESPACE, NameRefUtil.name(namespace));
  }

  static ResourceId table(String name) {
    return SystemNodeRegistry.resourceId(
        ENGINE, ResourceKind.RK_TABLE, NameRefUtil.name(PG_NAMESPACE, name));
  }

  static ResourceId view(String name) {
    return SystemNodeRegistry.resourceId(
        ENGINE, ResourceKind.RK_VIEW, NameRefUtil.name(PG_NAMESPACE, name));
  }

  static ResourceId type(String name) {
    return SystemNodeRegistry.resourceId(
        ENGINE, ResourceKind.RK_TYPE, NameRefUtil.name(PG_NAMESPACE, name));
  }

  static ResourceId type(NameRef name) {
    return SystemNodeRegistry.resourceId(ENGINE, ResourceKind.RK_TYPE, name);
  }
}

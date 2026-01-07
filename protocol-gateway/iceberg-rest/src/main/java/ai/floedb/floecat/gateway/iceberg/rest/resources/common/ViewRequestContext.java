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

package ai.floedb.floecat.gateway.iceberg.rest.resources.common;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.List;

public record ViewRequestContext(
    NamespaceRequestContext namespace, String view, ResourceId viewId) {

  public CatalogRequestContext catalog() {
    return namespace.catalog();
  }

  public List<String> namespacePath() {
    return namespace.namespacePath();
  }

  public String namespaceName() {
    return namespace.namespace();
  }
}

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

package ai.floedb.floecat.gateway.iceberg.minimal.services.namespace;

import ai.floedb.floecat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.floecat.catalog.rpc.Namespace;
import java.util.List;
import java.util.Map;

public interface NamespaceBackend {
  ListNamespacesResponse list(
      String prefix, List<String> parentPath, Integer pageSize, String pageToken);

  Namespace get(String prefix, List<String> namespacePath);

  void exists(String prefix, List<String> namespacePath);

  Namespace create(
      String prefix,
      List<String> namespacePath,
      Map<String, String> properties,
      String idempotencyKey);

  Namespace updateProperties(
      String prefix,
      List<String> namespacePath,
      Map<String, String> properties,
      String idempotencyKey);

  void delete(String prefix, List<String> namespacePath);
}

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

package ai.floedb.floecat.gateway.iceberg.minimal.services.view;

import ai.floedb.floecat.catalog.rpc.ListViewsResponse;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.List;
import java.util.Map;

public interface ViewBackend {
  ListViewsResponse list(
      String prefix, List<String> namespacePath, Integer pageSize, String pageToken);

  View get(String prefix, List<String> namespacePath, String viewName);

  void exists(String prefix, List<String> namespacePath, String viewName);

  void delete(String prefix, List<String> namespacePath, String viewName);

  View create(
      String prefix,
      List<String> namespacePath,
      String viewName,
      String sql,
      String dialect,
      List<String> creationSearchPath,
      List<SchemaColumn> outputColumns,
      Map<String, String> properties,
      String idempotencyKey);

  View update(
      String prefix,
      List<String> namespacePath,
      String viewName,
      String sql,
      Map<String, String> properties);

  void rename(
      String prefix,
      List<String> sourceNamespacePath,
      String sourceViewName,
      List<String> destinationNamespacePath,
      String destinationViewName);
}

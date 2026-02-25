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
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.scanner.utils;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.List;

/** Light helper overlay used only by scanner module tests. */
public class ScannerTestOverlay extends BaseTestCatalogOverlay {

  public ScannerTestOverlay addNode(GraphNode node) {
    super.addNode(node);
    return this;
  }

  public ScannerTestOverlay addRelation(ResourceId namespaceId, RelationNode node) {
    super.addRelation(namespaceId, node);
    return this;
  }

  public ScannerTestOverlay addFunction(ResourceId namespaceId, FunctionNode fn) {
    super.addFunction(namespaceId, fn);
    return this;
  }

  public ScannerTestOverlay setTableSchema(ResourceId tableId, List<SchemaColumn> schema) {
    super.setTableSchema(tableId, schema);
    return this;
  }
}

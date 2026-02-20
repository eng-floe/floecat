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

package ai.floedb.floecat.systemcatalog.utilities;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;

abstract class AbstractTestScanContextBuilder {

  protected final ResourceId catalogId;
  protected final TestCatalogOverlay overlay = new TestCatalogOverlay();

  protected AbstractTestScanContextBuilder(ResourceId catalogId) {
    this.catalogId = catalogId;
  }

  public SystemObjectScanContext build() {
    return new SystemObjectScanContext(
        overlay, NameRef.getDefaultInstance(), catalogId, EngineContext.empty());
  }
}

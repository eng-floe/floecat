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

package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.scanner.spi.ConstraintProvider;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.context.EngineContextProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.OptionalLong;

/** Constraint provider backed by immutable system catalog metadata. */
@ApplicationScoped
final class SystemConstraintProvider implements ConstraintProvider {

  private final SystemConstraintCatalog catalog;
  private final EngineContextProvider engineContextProvider;

  @Inject
  SystemConstraintProvider(
      SystemConstraintCatalog catalog, EngineContextProvider engineContextProvider) {
    this.catalog = catalog;
    this.engineContextProvider = engineContextProvider;
  }

  @Override
  public Optional<ConstraintSetView> constraints(ResourceId relationId, OptionalLong snapshotId) {
    EngineContext context =
        engineContextProvider.isPresent()
            ? engineContextProvider.engineContext()
            : EngineContext.empty();
    return catalog.constraints(context, relationId);
  }
}

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

package ai.floedb.floecat.systemcatalog.spi.decorator;

import ai.floedb.floecat.scanner.utils.EngineContext;

/**
 * Engine metadata decoration hook exposed by {@link
 * ai.floedb.floecat.service.query.catalog.UserObjectBundleService}.
 *
 * <p>Implementations are invoked during bundle construction and receive normalized engine kind +
 * version. Decoration remains optional and is not part of scanner implementations.
 */
public interface EngineMetadataDecorator {

  default void decorateNamespace(EngineContext ctx, NamespaceDecoration ns) {}

  default void decorateRelation(EngineContext ctx, RelationDecoration rel) {}

  default void decorateColumn(EngineContext ctx, ColumnDecoration col) {}

  default void decorateView(EngineContext ctx, ViewDecoration view) {}

  default void decorateType(EngineContext ctx, TypeDecoration type) {}

  default void decorateFunction(EngineContext ctx, FunctionDecoration fn) {}
}

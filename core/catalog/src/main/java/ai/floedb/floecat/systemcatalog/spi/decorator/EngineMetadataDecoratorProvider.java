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
import java.util.Optional;

/**
 * Provider that yields an engine-specific {@link EngineMetadataDecorator} for a normalized context.
 */
public interface EngineMetadataDecoratorProvider {

  /**
   * Returns the decorator for the provided engine context, if available.
   *
   * <p>The provided {@link EngineContext} is expected to already contain normalized kind/version
   * values.
   */
  Optional<EngineMetadataDecorator> decorator(EngineContext ctx);
}

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

package ai.floedb.floecat.systemcatalog.def;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import java.util.List;

/**
 * Common contract for all builtin catalog definitions (functions, operators, types, casts,
 * collations, aggregates).
 *
 * <p>This interface exists so that generic code (hint providers, registries, matching logic) can
 * operate uniformly across all builtin object kinds without switches or instanceof chains.
 *
 * <p>NOTE: Only shared semantic fields are included here. Kind-specific attributes (argument lists,
 * element types, etc.) remain in the concrete records.
 */
public interface SystemObjectDef {

  /** Returns the fully scoped builtin name (path + simple name). */
  NameRef name();

  /** Returns all engine-specific rules attached to this builtin object. */
  List<EngineSpecificRule> engineSpecific();

  /** Returns the builtin kind enumeration so generic code can branch without instanceof. */
  ResourceKind kind();
}

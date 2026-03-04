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

package ai.floedb.floecat.extensions.floedb.engine;

import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.systemcatalog.spi.types.EngineTypeMapper;
import ai.floedb.floecat.systemcatalog.spi.types.TypeLookup;
import ai.floedb.floecat.types.LogicalType;
import java.util.Optional;

/** Maps Floecat logical types to FloeDb builtin type. */
public final class FloeTypeMapper implements EngineTypeMapper {

  @Override
  public Optional<TypeNode> resolve(LogicalType t, TypeLookup lookup) {
    return switch (t.kind()) {
      case BOOLEAN -> lookup.findByName("pg_catalog", "bool");

      case INT16 -> lookup.findByName("pg_catalog", "int2");
      case INT32 -> lookup.findByName("pg_catalog", "int4");
      case INT64 -> lookup.findByName("pg_catalog", "int8");

      case FLOAT32 -> lookup.findByName("pg_catalog", "float4");
      case FLOAT64 -> lookup.findByName("pg_catalog", "float8");

      case STRING -> lookup.findByName("pg_catalog", "text");
      case BINARY -> lookup.findByName("pg_catalog", "bytea");
      case UUID -> lookup.findByName("pg_catalog", "uuid");

      case DATE -> lookup.findByName("pg_catalog", "date");
      case TIME -> lookup.findByName("pg_catalog", "time");
      case TIMESTAMP -> lookup.findByName("pg_catalog", "timestamp");

      case DECIMAL -> lookup.findByName("pg_catalog", "numeric");

      default -> Optional.empty();
    };
  }
}

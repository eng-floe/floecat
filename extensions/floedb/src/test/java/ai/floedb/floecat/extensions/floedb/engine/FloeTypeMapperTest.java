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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.systemcatalog.spi.types.TypeLookup;
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FloeTypeMapper}.
 *
 * <p>These tests validate that Floe logical types are mapped to the expected pg_catalog builtin
 * types via {@link TypeLookup}.
 */
class FloeTypeMapperTest {

  private FloeTypeMapper mapper;
  private FakeTypeLookup lookup;

  @BeforeEach
  void setUp() {
    mapper = new FloeTypeMapper();
    lookup =
        new FakeTypeLookup(
            Map.ofEntries(
                Map.entry("pg_catalog.int2", type("int2")),
                Map.entry("pg_catalog.int4", type("int4")),
                Map.entry("pg_catalog.int8", type("int8")),
                Map.entry("pg_catalog.float4", type("float4")),
                Map.entry("pg_catalog.float8", type("float8")),
                Map.entry("pg_catalog.text", type("text")),
                Map.entry("pg_catalog.bool", type("bool")),
                Map.entry("pg_catalog.numeric", type("numeric")),
                Map.entry("pg_catalog.bytea", type("bytea")),
                Map.entry("pg_catalog.uuid", type("uuid")),
                Map.entry("pg_catalog.date", type("date")),
                Map.entry("pg_catalog.time", type("time")),
                Map.entry("pg_catalog.timestamp", type("timestamp"))));
  }

  @Test
  void mapsBoolean() {
    assertMapped(LogicalType.of(LogicalKind.BOOLEAN), "bool");
  }

  @Test
  void mapsInt16() {
    assertMapped(LogicalType.of(LogicalKind.INT16), "int2");
  }

  @Test
  void mapsInt32() {
    assertMapped(LogicalType.of(LogicalKind.INT32), "int4");
  }

  @Test
  void mapsInt64() {
    assertMapped(LogicalType.of(LogicalKind.INT64), "int8");
  }

  @Test
  void mapsFloat32() {
    assertMapped(LogicalType.of(LogicalKind.FLOAT32), "float4");
  }

  @Test
  void mapsFloat64() {
    assertMapped(LogicalType.of(LogicalKind.FLOAT64), "float8");
  }

  @Test
  void mapsString() {
    assertMapped(LogicalType.of(LogicalKind.STRING), "text");
  }

  @Test
  void mapsBinary() {
    assertMapped(LogicalType.of(LogicalKind.BINARY), "bytea");
  }

  @Test
  void mapsUuid() {
    assertMapped(LogicalType.of(LogicalKind.UUID), "uuid");
  }

  @Test
  void mapsDate() {
    assertMapped(LogicalType.of(LogicalKind.DATE), "date");
  }

  @Test
  void mapsTime() {
    assertMapped(LogicalType.of(LogicalKind.TIME), "time");
  }

  @Test
  void mapsTimestamp() {
    assertMapped(LogicalType.of(LogicalKind.TIMESTAMP), "timestamp");
  }

  @Test
  void mapsDecimalToNumeric() {
    assertMapped(LogicalType.decimal(10, 2), "numeric");
  }

  @Test
  void unsupportedKindReturnsEmpty() {
    Optional<TypeNode> result =
        mapper.resolve(LogicalType.of(LogicalKind.BINARY), new FakeTypeLookup(Map.of()));
    assertThat(result).isEmpty();
  }

  private void assertMapped(LogicalType logicalType, String expectedPgName) {
    Optional<TypeNode> result = mapper.resolve(logicalType, lookup);
    assertThat(result).isPresent();
    assertThat(result.get().displayName()).isEqualTo(expectedPgName);
  }

  private static TypeNode type(String name) {
    return new TypeNode(null, 0L, null, "floedb", name, null, false, null, Map.of());
  }

  /** Minimal fake TypeLookup for unit testing. */
  private static final class FakeTypeLookup implements TypeLookup {
    private final Map<String, TypeNode> types;

    FakeTypeLookup(Map<String, TypeNode> types) {
      this.types = types;
    }

    @Override
    public Optional<TypeNode> findByName(String schema, String name) {
      return Optional.ofNullable(types.get(schema + "." + name));
    }
  }
}

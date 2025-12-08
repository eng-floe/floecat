package ai.floedb.floecat.catalog.builtin;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.BuiltinRegistry;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BuiltinCatalogProtoMapperTest {

  private static NameRef nr(String name) {
    return NameRef.newBuilder().setName(name).build();
  }

  @Test
  void roundTripCatalog() {

    // Rules: engine kind only inside our EngineSpecificRule; proto only carries floe_* and
    // properties
    var postgresRule = new EngineSpecificRule("postgres", "16.0", "", "", null, Map.of());

    var floeRule = new EngineSpecificRule("floedb", "", "", "", null, Map.of());

    // Function
    var function =
        new BuiltinFunctionDef(
            nr("abs"), List.of(nr("int4")), nr("int4"), false, false, List.of(postgresRule));

    // Operator
    var operator =
        new BuiltinOperatorDef(
            nr("+"), nr("int4"), nr("int4"), nr("int4"), true, true, List.of(postgresRule));

    // Types
    var scalarType = new BuiltinTypeDef(nr("int4"), "N", false, null, List.of(postgresRule));
    var arrayType = new BuiltinTypeDef(nr("_int4"), "A", true, nr("int4"), List.of());

    // Cast
    var cast =
        new BuiltinCastDef(
            nr("text2int4"),
            nr("text"),
            nr("int4"),
            BuiltinCastMethod.EXPLICIT,
            List.of(postgresRule));

    // Collation
    var collation = new BuiltinCollationDef(nr("default"), "en_US", List.of(postgresRule));

    // Aggregate
    var aggregate =
        new BuiltinAggregateDef(
            nr("sum"), List.of(nr("int4")), nr("int8"), nr("int8"), List.of(floeRule));

    var catalog =
        new BuiltinCatalogData(
            List.of(function),
            List.of(operator),
            List.of(scalarType, arrayType),
            List.of(cast),
            List.of(collation),
            List.of(aggregate));

    // Serialize
    BuiltinRegistry proto = BuiltinCatalogProtoMapper.toProto(catalog);

    // Validate proto fields
    assertThat(proto.getFunctionsCount()).isEqualTo(1);
    assertThat(proto.getFunctions(0).getName().getName()).isEqualTo("abs");

    assertThat(proto.getOperatorsCount()).isEqualTo(1);
    assertThat(proto.getOperators(0).getName().getName()).isEqualTo("+");

    assertThat(proto.getTypes(1).getElementType().getName()).isEqualTo("int4");

    assertThat(proto.getCasts(0).getMethod().toLowerCase()).isEqualTo("explicit");

    assertThat(proto.getAggregates(0).getName().getName()).isEqualTo("sum");

    // EngineSpecific only carries Floe variants + properties
    assertThat(proto.getFunctions(0).getEngineSpecificCount()).isEqualTo(1);
    assertThat(proto.getAggregates(0).getEngineSpecificCount()).isEqualTo(1);

    // Round trip back to domain
    BuiltinCatalogData roundTrip = BuiltinCatalogProtoMapper.fromProto(proto);

    // Cannot assert full equality: proto drops engineKind/min/max
    assertThat(roundTrip)
        .usingRecursiveComparison()
        .ignoringFields(
            // drop engineKind/min/max everywhere in engineSpecific
            "functions.engineSpecific.engineKind",
            "functions.engineSpecific.minVersion",
            "functions.engineSpecific.maxVersion",
            "operators.engineSpecific.engineKind",
            "operators.engineSpecific.minVersion",
            "operators.engineSpecific.maxVersion",
            "types.engineSpecific.engineKind",
            "types.engineSpecific.minVersion",
            "types.engineSpecific.maxVersion",
            "casts.engineSpecific.engineKind",
            "casts.engineSpecific.minVersion",
            "casts.engineSpecific.maxVersion",
            "collations.engineSpecific.engineKind",
            "collations.engineSpecific.minVersion",
            "collations.engineSpecific.maxVersion",
            "aggregates.engineSpecific.engineKind",
            "aggregates.engineSpecific.minVersion",
            "aggregates.engineSpecific.maxVersion")
        .isEqualTo(catalog);
  }
}

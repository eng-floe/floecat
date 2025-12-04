package ai.floedb.metacat.catalog.builtin;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.metacat.query.rpc.BuiltinRegistry;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BuiltinCatalogProtoMapperTest {

  @Test
  void roundTripCatalog() {

    // Rules: engine kind only inside our EngineSpecificRule; proto only carries floe_* and
    // properties
    var postgresRule =
        new EngineSpecificRule("postgres", "16.0", "", null, null, null, null, null, Map.of());

    var floeRule = new EngineSpecificRule("floedb", "", "", null, null, null, null, null, Map.of());

    // Function
    var function =
        new BuiltinFunctionDef("abs", List.of("int4"), "int4", false, false, List.of(postgresRule));

    // Operator
    var operator =
        new BuiltinOperatorDef("+", "int4", "int4", "int4", true, true, List.of(postgresRule));

    // Types
    var scalarType = new BuiltinTypeDef("int4", "N", false, null, List.of(postgresRule));
    var arrayType = new BuiltinTypeDef("_int4", "A", true, "int4", List.of());

    // Cast
    var cast =
        new BuiltinCastDef("text", "int4", BuiltinCastMethod.EXPLICIT, List.of(postgresRule));

    // Collation
    var collation = new BuiltinCollationDef("default", "en_US", List.of(postgresRule));

    // Aggregate
    var aggregate =
        new BuiltinAggregateDef("sum", List.of("int4"), "int8", "int8", List.of(floeRule));

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
    assertThat(proto.getFunctions(0).getName()).isEqualTo("abs");

    assertThat(proto.getOperatorsCount()).isEqualTo(1);
    assertThat(proto.getOperators(0).getName()).isEqualTo("+");

    assertThat(proto.getTypes(1).getElementType()).isEqualTo("int4");

    assertThat(proto.getCasts(0).getMethod()).isEqualTo("explicit");

    assertThat(proto.getAggregates(0).getName()).isEqualTo("sum");

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

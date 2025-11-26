package ai.floedb.metacat.catalog.builtin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class BuiltinCatalogProtoMapperTest {

  @Test
  void roundTripCatalog() {
    var function = new BuiltinFunctionDef("abs", List.of("int4"), "int4", false, false, true, true);
    var operator = new BuiltinOperatorDef("+", "int4", "int4", "int4_add");
    var scalarType = new BuiltinTypeDef("int4", 23, "N", false, null);
    var arrayType = new BuiltinTypeDef("_int4", 1007, "A", true, "int4");
    var cast = new BuiltinCastDef("text", "int4", BuiltinCastMethod.EXPLICIT);
    var collation = new BuiltinCollationDef("default", "en_US");
    var aggregate =
        new BuiltinAggregateDef(
            "sum", List.of("int4"), "int8", "int8", "int4_sum_state", "int4_sum_final");

    var catalog =
        new BuiltinCatalogData(
            "v1",
            List.of(function),
            List.of(operator),
            List.of(scalarType, arrayType),
            List.of(cast),
            List.of(collation),
            List.of(aggregate));

    var proto = BuiltinCatalogProtoMapper.toProto(catalog);
    assertThat(proto.getVersion()).isEqualTo("v1");
    assertThat(proto.getFunctionsCount()).isEqualTo(1);
    assertThat(proto.getFunctions(0).getIsStrict()).isTrue();
    assertThat(proto.getOperators(0).getFunctionName()).isEqualTo("int4_add");
    assertThat(proto.getTypes(1).getElementType()).isEqualTo("int4");
    assertThat(proto.getCasts(0).getMethod()).isEqualTo("explicit");
    assertThat(proto.getAggregates(0).getStateFn()).isEqualTo("int4_sum_state");

    var roundTrip = BuiltinCatalogProtoMapper.fromProto(proto);
    assertThat(roundTrip).isEqualTo(catalog);
  }
}

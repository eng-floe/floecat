package ai.floedb.metacat.catalog.builtin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.metacat.common.rpc.NameRef;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests covering the existing validation rules enforced by {@link BuiltinCatalogValidator}.
 */
class BuiltinCatalogValidatorTest {

  private static NameRef pg(String name) {
    return NameRef.newBuilder().addPath("pg_catalog").setName(name).build();
  }

  @Test
  void validCatalogHasNoErrors() {
    assertTrue(BuiltinCatalogValidator.validate(validCatalog()).isEmpty());
  }

  @Test
  void nullCatalogFails() {
    assertEquals(List.of("catalog.null"), BuiltinCatalogValidator.validate(null));
  }

  @Test
  void duplicateTypeNamesAreDetected() {
    BuiltinCatalogData base = validCatalog();
    var types = new ArrayList<>(base.types());
    types.add(new BuiltinTypeDef(pg("int4"), "N", false, null, List.of()));

    var catalog =
        new BuiltinCatalogData(
            base.functions(),
            base.operators(),
            types,
            base.casts(),
            base.collations(),
            base.aggregates());

    assertTrue(
        BuiltinCatalogValidator.validate(catalog).contains("type.duplicate:pg_catalog.int4"));
  }

  @Test
  void functionsMustReferenceKnownTypes() {
    BuiltinCatalogData base = validCatalog();
    var functions =
        List.of(
            new BuiltinFunctionDef(
                pg("identity"), List.of(pg("unknown")), pg("int4"), false, false, List.of()));
    var catalog =
        new BuiltinCatalogData(
            functions,
            base.operators(),
            base.types(),
            base.casts(),
            base.collations(),
            base.aggregates());

    var errors = BuiltinCatalogValidator.validate(catalog);
    assertTrue(errors.contains("function.arg:pg_catalog.identity.type.unknown:pg_catalog.unknown"));
  }

  @Test
  void duplicateCastPairsReported() {
    BuiltinCatalogData base = validCatalog();
    var casts = new ArrayList<>(base.casts());
    casts.add(
        new BuiltinCastDef(
            pg("int42int4"), pg("int4"), pg("int4"), BuiltinCastMethod.ASSIGNMENT, List.of()));

    var catalog =
        new BuiltinCatalogData(
            base.functions(),
            base.operators(),
            base.types(),
            casts,
            base.collations(),
            base.aggregates());

    assertTrue(
        BuiltinCatalogValidator.validate(catalog).stream()
            .anyMatch(error -> error.contains("cast.duplicate")));
  }

  @Test
  void duplicateCollationsAreRejected() {
    BuiltinCatalogData base = validCatalog();
    var collations = new ArrayList<>(base.collations());
    collations.add(new BuiltinCollationDef(pg("default"), "en_US", List.of()));

    var catalog =
        new BuiltinCatalogData(
            base.functions(),
            base.operators(),
            base.types(),
            base.casts(),
            collations,
            base.aggregates());

    assertTrue(
        BuiltinCatalogValidator.validate(catalog)
            .contains("collation.duplicate:pg_catalog.default"));
  }

  private static BuiltinCatalogData validCatalog() {
    var type = new BuiltinTypeDef(pg("int4"), "N", false, null, List.of());
    var fn =
        new BuiltinFunctionDef(
            pg("identity"), List.of(pg("int4")), pg("int4"), false, false, List.of());
    var op =
        new BuiltinOperatorDef(
            pg("plus"), pg("int4"), pg("int4"), pg("int4"), true, true, List.of());
    var cast =
        new BuiltinCastDef(
            pg("int42int4"), pg("int4"), pg("int4"), BuiltinCastMethod.ASSIGNMENT, List.of());
    var coll = new BuiltinCollationDef(pg("default"), "en_US", List.of());
    var agg =
        new BuiltinAggregateDef(pg("sum"), List.of(pg("int4")), pg("int4"), pg("int4"), List.of());
    return new BuiltinCatalogData(
        List.of(fn), List.of(op), List.of(type), List.of(cast), List.of(coll), List.of(agg));
  }
}

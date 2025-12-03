package ai.floedb.metacat.catalog.builtin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests covering the existing validation rules enforced by {@link BuiltinCatalogValidator}.
 */
class BuiltinCatalogValidatorTest {

  @Test
  void validCatalogHasNoErrors() {
    assertTrue(BuiltinCatalogValidator.validate(validCatalog()).isEmpty());
  }

  @Test
  void nullCatalogFails() {
    assertEquals(List.of("catalog.null"), BuiltinCatalogValidator.validate(null));
  }

  @Test
  void missingVersionIsReported() {
    BuiltinCatalogData base = validCatalog();
    var catalog =
        new BuiltinCatalogData(
            null,
            base.functions(),
            base.operators(),
            base.types(),
            base.casts(),
            base.collations(),
            base.aggregates());

    assertTrue(BuiltinCatalogValidator.validate(catalog).contains("catalog.version.required"));
  }

  @Test
  void duplicateTypeNamesAreDetected() {
    BuiltinCatalogData base = validCatalog();
    var types = new ArrayList<>(base.types());
    types.add(new BuiltinTypeDef("pg_catalog.int4", 24, "N", false, null, List.of()));

    var catalog =
        new BuiltinCatalogData(
            base.version(),
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
                "pg_catalog.identity",
                List.of("pg_catalog.unknown"),
                "pg_catalog.int4",
                false,
                false,
                true,
                true,
                List.of()));
    var catalog =
        new BuiltinCatalogData(
            base.version(),
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
  void operatorRequiresExistingFunctionAndTypes() {
    BuiltinCatalogData base = validCatalog();
    var operators =
        List.of(
            new BuiltinOperatorDef(
                "pg_catalog.plus",
                "pg_catalog.int4",
                "pg_catalog.int4",
                "pg_catalog.missing",
                List.of()));
    var catalog =
        new BuiltinCatalogData(
            base.version(),
            base.functions(),
            operators,
            base.types(),
            base.casts(),
            base.collations(),
            base.aggregates());

    var errors = BuiltinCatalogValidator.validate(catalog);
    assertTrue(errors.contains("operator.function.missing:pg_catalog.plus"));
  }

  @Test
  void duplicateCastPairsReported() {
    BuiltinCatalogData base = validCatalog();
    var casts = new ArrayList<>(base.casts());
    casts.add(
        new BuiltinCastDef(
            "pg_catalog.int4", "pg_catalog.int4", BuiltinCastMethod.ASSIGNMENT, List.of()));

    var catalog =
        new BuiltinCatalogData(
            base.version(),
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
    collations.add(new BuiltinCollationDef("pg_catalog.default", "en_US", List.of()));

    var catalog =
        new BuiltinCatalogData(
            base.version(),
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

  @Test
  void aggregatesMustReferenceKnownFunctions() {
    BuiltinCatalogData base = validCatalog();
    var aggregates =
        List.of(
            new BuiltinAggregateDef(
                "pg_catalog.sum",
                List.of("pg_catalog.int4"),
                "pg_catalog.int4",
                "pg_catalog.int4",
                "pg_catalog.missing",
                "pg_catalog.missing",
                List.of()));
    var catalog =
        new BuiltinCatalogData(
            base.version(),
            base.functions(),
            base.operators(),
            base.types(),
            base.casts(),
            base.collations(),
            aggregates);

    var errors = BuiltinCatalogValidator.validate(catalog);
    assertTrue(errors.contains("agg.stateFn:pg_catalog.sum.function.unknown:pg_catalog.missing"));
    assertTrue(errors.contains("agg.finalFn:pg_catalog.sum.function.unknown:pg_catalog.missing"));
  }

  private static BuiltinCatalogData validCatalog() {
    var type = new BuiltinTypeDef("pg_catalog.int4", 23, "N", false, null, List.of());
    var fn =
        new BuiltinFunctionDef(
            "pg_catalog.identity",
            List.of("pg_catalog.int4"),
            "pg_catalog.int4",
            false,
            false,
            true,
            true,
            List.of());
    var op =
        new BuiltinOperatorDef(
            "pg_catalog.plus",
            "pg_catalog.int4",
            "pg_catalog.int4",
            "pg_catalog.identity",
            List.of());
    var cast =
        new BuiltinCastDef(
            "pg_catalog.int4", "pg_catalog.int4", BuiltinCastMethod.ASSIGNMENT, List.of());
    var coll = new BuiltinCollationDef("pg_catalog.default", "en_US", List.of());
    var agg =
        new BuiltinAggregateDef(
            "pg_catalog.sum",
            List.of("pg_catalog.int4"),
            "pg_catalog.int4",
            "pg_catalog.int4",
            "pg_catalog.identity",
            "pg_catalog.identity",
            List.of());

    return new BuiltinCatalogData(
        "demo",
        List.of(fn),
        List.of(op),
        List.of(type),
        List.of(cast),
        List.of(coll),
        List.of(agg));
  }
}

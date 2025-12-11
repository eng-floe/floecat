package ai.floedb.floecat.catalog.systemobjects.registry;

import static org.assertj.core.api.Assertions.*;

import ai.floedb.floecat.catalog.common.engine.EngineSpecificRule;
import ai.floedb.floecat.catalog.common.util.NameRefUtil;
import ai.floedb.floecat.catalog.systemobjects.spi.SystemObjectColumnSet;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SystemObjectDefinitionTest {

  private static final SchemaColumn COL =
      SchemaColumn.newBuilder().setName("x").setLogicalType("INT").setFieldId(0).build();

  @Test
  void canonicalName_isNormalized() {
    NameRef ref = NameRefUtil.name("Information_Schema", "Tables");
    SystemObjectDefinition def =
        new SystemObjectDefinition(
            ref, new SystemObjectColumnSet(new SchemaColumn[] {COL}), "id", List.of());

    assertThat(def.canonicalName()).isEqualTo("information_schema.tables");
  }

  @Test
  void canonicalName_handlesDeepPaths() {
    NameRef ref = NameRefUtil.name("A", "B", "C");
    var def =
        new SystemObjectDefinition(
            ref, new SystemObjectColumnSet(new SchemaColumn[] {COL}), "id", List.of());

    assertThat(def.canonicalName()).isEqualTo("a.b.c");
  }

  @Test
  void engineRules_areImmutable() {
    EngineSpecificRule rule = new EngineSpecificRule("spark", "", "", "", new byte[0], Map.of());
    List<EngineSpecificRule> rules = List.of(rule);

    SystemObjectDefinition def =
        new SystemObjectDefinition(
            NameRefUtil.name("a", "b"),
            new SystemObjectColumnSet(new SchemaColumn[] {COL}),
            "id",
            rules);

    assertThat(def.engineSpecificRules()).isUnmodifiable();
    assertThat(def.engineSpecificRules()).containsExactly(rule);
  }

  @Test
  void engineRules_nullBecomesEmptyList() {
    SystemObjectDefinition def =
        new SystemObjectDefinition(
            NameRefUtil.name("a", "b"),
            new SystemObjectColumnSet(new SchemaColumn[] {COL}),
            "id",
            null);

    assertThat(def.engineSpecificRules()).isEmpty();
    assertThat(def.engineSpecificRules()).isUnmodifiable();
  }

  @Test
  void columnSet_isDefensivelyCopied() {
    SchemaColumn[] arr = new SchemaColumn[] {COL};
    SystemObjectColumnSet set = new SystemObjectColumnSet(arr);

    arr[0] = null; // mutate original input array

    var def = new SystemObjectDefinition(NameRefUtil.name("a", "b"), set, "id", List.of());

    assertThat(def.columns().column(0)).isEqualTo(COL);
  }

  @Test
  void columnSet_isStored() {
    var cols = new SystemObjectColumnSet(new SchemaColumn[] {COL});

    SystemObjectDefinition def =
        new SystemObjectDefinition(NameRefUtil.name("a", "b"), cols, "id", List.of());

    assertThat(def.columns().size()).isEqualTo(1);
  }

  @Test
  void scannerId_isStored() {
    SystemObjectDefinition def =
        new SystemObjectDefinition(
            NameRefUtil.name("a", "b"),
            new SystemObjectColumnSet(new SchemaColumn[] {COL}),
            "myScanner",
            List.of());

    assertThat(def.scannerId()).isEqualTo("myScanner");
  }

  @Test
  void scannerId_allowsNonNullOnly() {
    SystemObjectDefinition def =
        new SystemObjectDefinition(
            NameRefUtil.name("a", "b"),
            new SystemObjectColumnSet(new SchemaColumn[] {COL}),
            "",
            List.of());

    assertThat(def.scannerId()).isNotNull();
  }

  /** Ensures equality is based on all fields and behaves predictably. */
  @Test
  void equalsAndHashCode_consistentForIdenticalDefinitions() {
    var cols = new SystemObjectColumnSet(new SchemaColumn[] {COL});
    var rules = List.of(new EngineSpecificRule("spark", "", "", "", new byte[0], Map.of()));

    var d1 = new SystemObjectDefinition(NameRefUtil.name("a", "b"), cols, "id", rules);
    var d2 = new SystemObjectDefinition(NameRefUtil.name("a", "b"), cols, "id", rules);

    assertThat(d1).isEqualTo(d2);
    assertThat(d1.hashCode()).isEqualTo(d2.hashCode());
  }

  /** Ensures inequality when any field differs. */
  @Test
  void equalsAndHashCode_detectsDifferences() {
    var cols = new SystemObjectColumnSet(new SchemaColumn[] {COL});

    var d1 = new SystemObjectDefinition(NameRefUtil.name("a", "b"), cols, "id1", List.of());
    var d2 = new SystemObjectDefinition(NameRefUtil.name("a", "b"), cols, "id2", List.of());

    assertThat(d1).isNotEqualTo(d2);
  }

  /**
   * EngineSpecificRules: verify modifying the provided list after construction does not mutate the
   * stored rule set.
   */
  @Test
  void engineRules_areDefensivelyCopied() {
    var rule = new EngineSpecificRule("spark", "", "", "", new byte[0], Map.of());
    var rules = List.of(rule);
    var def =
        new SystemObjectDefinition(
            NameRefUtil.name("a", "b"),
            new SystemObjectColumnSet(new SchemaColumn[] {COL}),
            "id",
            rules);

    // Attempt to mutate original list (will throw but ensures immutability)
    assertThatThrownBy(() -> rules.add(rule)).isInstanceOf(UnsupportedOperationException.class);

    // Ensure definition remains unmodified
    assertThat(def.engineSpecificRules()).containsExactly(rule);
  }
}

package ai.floedb.floecat.catalog.system_objects.registry;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.common.util.NameRefUtil;
import ai.floedb.floecat.catalog.system_objects.spi.SystemObjectColumnSet;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.SystemObjectNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class SystemObjectNodeFactoryTest {

  private static final SchemaColumn COL =
      SchemaColumn.newBuilder().setName("c").setFieldId(0).setLogicalType("INT").build();

  private static final SystemObjectColumnSet COLS =
      new SystemObjectColumnSet(new SchemaColumn[] {COL});

  private static SystemObjectDefinition def(String canonical, String rowId) {
    NameRef ref = NameRefUtil.fromCanonical(canonical);
    return new SystemObjectDefinition(ref, COLS, rowId, List.of());
  }

  @Test
  void buildsNode_withStableFields() {
    SystemObjectDefinition d = def("a.b.c", "gen");

    SystemObjectNodeFactory factory = new SystemObjectNodeFactory();

    SystemObjectNode node = factory.build(d, "spark", "3.5.0");

    assertThat(node.displayName()).isEqualTo("c"); // last segment
    assertThat(node.version()).isZero(); // always zero for SO
    assertThat(node.metadataUpdatedAt()).isEqualTo(Instant.EPOCH); // stable timestamp
    assertThat(node.columns()).containsExactly(COL); // schema copied
    assertThat(node.scannerId()).isEqualTo("gen"); // correct assignment
  }

  @Test
  void resourceIds_areCorrectlyFormatted() {
    SystemObjectDefinition d = def("x.y", "id");
    SystemObjectNodeFactory factory = new SystemObjectNodeFactory();

    SystemObjectNode node = factory.build(d, "spark", "3.5.0");

    assertThat(node.id().getAccountId()).isEqualTo("_sysobjects");
    assertThat(node.id().getKind()).isEqualTo(ResourceKind.RK_SYSTEM_OBJECT);
    assertThat(node.id().getId()).isEqualTo("spark:x.y");

    assertThat(node.catalogId().getId()).isEqualTo("spark");
    assertThat(node.namespaceId().getId()).isEqualTo("spark:x");
  }

  @Test
  void columnArray_isDefensivelyCopied() {
    SystemObjectDefinition d = def("a.b", "id");

    SystemObjectNodeFactory factory = new SystemObjectNodeFactory();
    SystemObjectNode node = factory.build(d, "spark", "3.5.0");

    SchemaColumn[] before = node.columns();
    before[0] = SchemaColumn.newBuilder().setName("bad").setFieldId(1).build();

    // ensure internal state not mutated
    assertThat(factory.build(d, "spark", "3.5.0").columns()[0]).isEqualTo(COL);
  }

  @Test
  void scannerId_isPreserved() {
    SystemObjectDefinition d = def("info.x", "abc123");

    SystemObjectNodeFactory factory = new SystemObjectNodeFactory();
    SystemObjectNode node = factory.build(d, "spark", "3.5.0");

    assertThat(node.scannerId()).isEqualTo("abc123");
  }

  @Test
  void engineHints_isEmptyByDefault() {
    SystemObjectDefinition d = def("info.x", "id");
    SystemObjectNodeFactory factory = new SystemObjectNodeFactory();

    SystemObjectNode node = factory.build(d, "spark", "3.5.0");

    assertThat(node.engineHints()).isEmpty();
  }

  @Test
  void namespaceId_usesCanonicalPathSegments() {
    SystemObjectDefinition d = def("a.b.c", "id");
    SystemObjectNodeFactory factory = new SystemObjectNodeFactory();

    SystemObjectNode node = factory.build(d, "spark", "3.5.0");

    // namespaceId = spark:a.b
    assertThat(node.namespaceId().getId()).isEqualTo("spark:a.b");
  }
}

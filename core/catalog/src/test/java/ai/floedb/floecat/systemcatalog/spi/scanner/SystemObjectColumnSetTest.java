package ai.floedb.floecat.systemcatalog.spi.scanner;

import static org.assertj.core.api.Assertions.*;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.List;
import org.junit.jupiter.api.Test;

class SystemObjectColumnSetTest {

  @Test
  void storesColumnsDefensively() {
    SchemaColumn col =
        SchemaColumn.newBuilder().setName("x").setLogicalType("INT").setFieldId(0).build();

    List<SchemaColumn> list = List.of(col);
    SystemObjectColumnSet set = new SystemObjectColumnSet(list);

    // try mutating original array
    assertThatThrownBy(() -> list.set(0, null)).isInstanceOf(UnsupportedOperationException.class);

    assertThat(set.column(0)).isSameAs(col);
  }

  @Test
  void returnedColumnsAreImmutable() {
    SchemaColumn col =
        SchemaColumn.newBuilder().setName("x").setLogicalType("INT").setFieldId(0).build();

    SystemObjectColumnSet set = new SystemObjectColumnSet(List.of(col));

    List<SchemaColumn> leaked = set.columns();
    assertThatThrownBy(() -> leaked.set(0, null)).isInstanceOf(UnsupportedOperationException.class);

    assertThat(set.column(0)).isNotNull();
  }

  @Test
  void sizeMatchesDefinition() {
    SchemaColumn col =
        SchemaColumn.newBuilder().setName("x").setLogicalType("INT").setFieldId(0).build();

    SystemObjectColumnSet set = new SystemObjectColumnSet(List.of(col));

    assertThat(set.size()).isEqualTo(1);
  }
}

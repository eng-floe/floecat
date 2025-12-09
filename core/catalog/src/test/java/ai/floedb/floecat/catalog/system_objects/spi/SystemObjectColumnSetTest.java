package ai.floedb.floecat.catalog.system_objects.spi;

import static org.assertj.core.api.Assertions.*;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import org.junit.jupiter.api.Test;

class SystemObjectColumnSetTest {

  @Test
  void storesColumnsDefensively() {
    SchemaColumn col =
        SchemaColumn.newBuilder().setName("x").setLogicalType("INT").setFieldId(0).build();

    SchemaColumn[] arr = new SchemaColumn[] {col};
    SystemObjectColumnSet set = new SystemObjectColumnSet(arr);

    // mutate original array
    arr[0] = null;

    assertThat(set.column(0)).isSameAs(col);
  }

  @Test
  void returnedColumnsAreImmutable() {
    SchemaColumn col =
        SchemaColumn.newBuilder().setName("x").setLogicalType("INT").setFieldId(0).build();

    SystemObjectColumnSet set = new SystemObjectColumnSet(new SchemaColumn[] {col});

    SchemaColumn[] leaked = set.columns();
    leaked[0] = null; // attempt mutation

    assertThat(set.column(0)).isNotNull();
  }

  @Test
  void sizeMatchesDefinition() {
    SchemaColumn col =
        SchemaColumn.newBuilder().setName("x").setLogicalType("INT").setFieldId(0).build();

    SystemObjectColumnSet set = new SystemObjectColumnSet(new SchemaColumn[] {col});

    assertThat(set.size()).isEqualTo(1);
  }
}

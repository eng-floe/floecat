package ai.floedb.floecat.catalog.system_objects.information_schema;

import static org.assertj.core.api.Assertions.*;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import org.junit.jupiter.api.Test;

class ColumnsScannerTest {

  @Test
  void schema_isCorrect() {
    assertThat(new ColumnsScanner().schema())
        .extracting(SchemaColumn::getName)
        .containsExactly(
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "data_type",
            "ordinal_position");
  }
}

package ai.floedb.floecat.service.query.system;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.system.rpc.SystemTableRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import org.junit.jupiter.api.Test;

class SystemRowMappersTest {

  @Test
  void toProto_mapsAllValuesInOrder() {
    SystemObjectRow row =
        new SystemObjectRow(new Object[] {"cat", "schema", "table", "BASE TABLE"});

    SystemTableRow proto = SystemRowMappers.toProto(row);

    assertThat(proto.getValuesList()).containsExactly("cat", "schema", "table", "BASE TABLE");
  }

  @Test
  void toProto_convertsNullValuesToEmptyString() {
    SystemObjectRow row = new SystemObjectRow(new Object[] {"cat", null, "table", null});

    SystemTableRow proto = SystemRowMappers.toProto(row);

    assertThat(proto.getValuesList()).containsExactly("cat", "", "table", "");
  }

  @Test
  void toProto_handlesEmptyRow() {
    SystemObjectRow row = new SystemObjectRow(new Object[] {});

    SystemTableRow proto = SystemRowMappers.toProto(row);

    assertThat(proto.getValuesList()).isEmpty();
  }
}

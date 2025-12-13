package ai.floedb.floecat.systemcatalog.spi.scanner;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SystemObjectRowTest {

  @Test
  void storesRawArrayWithoutCopying() {
    Object[] arr = new Object[] {"a", 1};

    SystemObjectRow row = new SystemObjectRow(arr);

    assertThat(row.values()).isSameAs(arr);
  }

  @Test
  void mutatingSourceArrayIsReflectedInRow() {
    Object[] arr = new Object[] {"a", 1};
    SystemObjectRow row = new SystemObjectRow(arr);

    arr[0] = "b";

    assertThat(row.values()).isSameAs(arr);
    assertThat(row.values()[0]).isEqualTo("b");
  }

  @Test
  void constructor_allowsNullValues() {
    Object[] arr = new Object[] {"a", null, 1};
    SystemObjectRow row = new SystemObjectRow(arr);

    assertThat(row.values()).containsNull();
  }
}

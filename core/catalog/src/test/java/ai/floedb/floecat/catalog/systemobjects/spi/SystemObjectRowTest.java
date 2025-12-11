package ai.floedb.floecat.catalog.systemobjects.spi;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SystemObjectRowTest {

  @Test
  void storesRawArrayWithoutCopying() {
    Object[] arr = new Object[] {"a", 1};

    SystemObjectRow row = new SystemObjectRow(arr);

    assertThat(row.values()).isSameAs(arr);
  }
}

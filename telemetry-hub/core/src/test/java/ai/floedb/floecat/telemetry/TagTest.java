package ai.floedb.floecat.telemetry;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TagTest {

  @Test
  void rejectsBlankKey() {
    assertThatThrownBy(() -> Tag.of(" ", "value"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("key must not be blank");
  }

  @Test
  void rejectsNullValue() {
    assertThatThrownBy(() -> Tag.of("key", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("value");
  }
}

package ai.floedb.floecat.telemetry;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class MetricIdTest {

  @Test
  void rejectsBlankName() {
    assertThatThrownBy(() -> new MetricId("   ", MetricType.COUNTER, "count", "v1", "core"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("name");
  }

  @Test
  void rejectsBlankUnit() {
    assertThatThrownBy(() -> new MetricId("metric", MetricType.COUNTER, "  ", "v1", "core"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unit");
  }
}

package ai.floedb.metacat.service.query.graph.builtin;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.metacat.catalog.builtin.EngineSpecificRule;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EngineSpecificMatcherTest {

  @Test
  void numericSegmentsCompareNaturally() {
    EngineSpecificRule rule =
        new EngineSpecificRule("", "10", "", null, null, null, null, null, Map.of());
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "2")).isFalse();
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "10")).isTrue();
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "11")).isTrue();
  }

  @Test
  void alphanumericVersionsHandledConsistently() {
    EngineSpecificRule rule =
        new EngineSpecificRule("", "", "16.1", null, null, null, null, null, Map.of());
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "16.1beta")).isFalse();
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "16.0beta2")).isTrue();
  }
}

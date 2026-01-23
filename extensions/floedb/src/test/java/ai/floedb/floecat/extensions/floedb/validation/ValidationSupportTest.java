package ai.floedb.floecat.extensions.floedb.validation;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.engine.VersionIntervals;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import com.google.protobuf.Empty;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ValidationSupportTest {

  @Test
  void detectsOverlapWithinSameEngineKind() {
    List<ValidationIssue> issues = new ArrayList<>();
    var ruleA = rule("floedb", "1", "2");
    var ruleB = rule("floedb", "1.5", "3");
    ValidationSupport.detectRuleOverlaps(
        List.of(decoded(ruleA), decoded(ruleB)), "type", "ctx", issues);

    assertThat(issues).extracting(ValidationIssue::code).contains("floe.rules.overlap");
  }

  @Test
  void detectsWildcardOverlapAgainstSpecific() {
    List<ValidationIssue> issues = new ArrayList<>();
    var wildcard = rule("", "1", "3");
    var specific = rule("floedb", "2", "4");
    ValidationSupport.detectRuleOverlaps(
        List.of(decoded(wildcard), decoded(specific)), "type", "ctx", issues);

    assertThat(issues).extracting(ValidationIssue::code).contains("floe.rules.overlap");
  }

  @Test
  void doesNotReportNonOverlappingRules() {
    List<ValidationIssue> issues = new ArrayList<>();
    var ruleA = rule("floedb", "1", "2");
    var ruleB = rule("floedb", "3", "4");
    ValidationSupport.detectRuleOverlaps(
        List.of(decoded(ruleA), decoded(ruleB)), "type", "ctx", issues);

    assertThat(issues).isEmpty();
  }

  @Test
  void treatsTouchingIntervalsAsOverlapping() {
    List<ValidationIssue> issues = new ArrayList<>();
    var ruleA = rule("floedb", "1", "2");
    var ruleB = rule("floedb", "2", "3");
    ValidationSupport.detectRuleOverlaps(
        List.of(decoded(ruleA), decoded(ruleB)), "type", "ctx", issues);

    assertThat(issues).isNotEmpty();
  }

  @Test
  void objectExistenceIntervalsMergeRules() {
    var rules = List.of(rule("floedb", "1", "2"), rule("floedb", "2", "4"));
    var intervals = ValidationSupport.objectExistenceIntervals(rules);

    assertThat(intervals)
        .containsExactly(
            new VersionIntervals.VersionInterval(
                VersionIntervals.VersionBound.finite("1"),
                VersionIntervals.VersionBound.finite("4")));
  }

  private static ValidationSupport.DecodedRule<Empty> decoded(EngineSpecificRule rule) {
    return new ValidationSupport.DecodedRule<>(
        rule, VersionIntervals.VersionInterval.fromRule(rule), Empty.getDefaultInstance());
  }

  private static EngineSpecificRule rule(String engineKind, String min, String max) {
    return new EngineSpecificRule(engineKind, min, max, "type", new byte[] {1}, Map.of());
  }
}

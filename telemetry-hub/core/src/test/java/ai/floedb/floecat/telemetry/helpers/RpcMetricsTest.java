package ai.floedb.floecat.telemetry.helpers;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.telemetry.Observability.Category;
import ai.floedb.floecat.telemetry.ObservationScope;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.TestObservability;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class RpcMetricsTest {
  @Test
  void recordsRequestsAndTracksActiveGauge() {
    TestObservability observability = new TestObservability();
    RpcMetrics metrics = new RpcMetrics(observability, "svc", "op", Tag.of(TagKey.ACCOUNT, "acct"));

    Supplier<? extends Number> gauge = observability.gauge(Telemetry.Metrics.RPC_ACTIVE);
    assertThat(gauge).isNotNull();

    metrics.incrementActiveRequests();
    assertThat(gauge.get().doubleValue()).isEqualTo(1d);

    metrics.decrementActiveRequests();
    assertThat(gauge.get().doubleValue()).isEqualTo(0d);

    metrics.recordRequest("acct", "ok");
    assertThat(observability.counterValue(Telemetry.Metrics.RPC_REQUESTS)).isEqualTo(1d);
  }

  @Test
  void observationScopeIncrementsGaugeAndRecordsScope() {
    TestObservability observability = new TestObservability();
    RpcMetrics metrics = new RpcMetrics(observability, "svc", "op");

    ObservationScope scope =
        metrics.observe(Tag.of(TagKey.ACCOUNT, "acct"), Tag.of(TagKey.STATUS, "ok"));
    scope.success();
    scope.close();

    assertThat(observability.scopes()).containsKey(Category.RPC.name());
  }
}

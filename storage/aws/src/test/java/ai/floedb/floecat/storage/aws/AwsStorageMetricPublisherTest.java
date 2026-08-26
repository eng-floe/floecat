/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.floedb.floecat.storage.aws;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.telemetry.StorageTelemetry;
import ai.floedb.floecat.telemetry.StorageTelemetry.Backend;
import ai.floedb.floecat.telemetry.StorageTelemetry.Call;
import ai.floedb.floecat.telemetry.StorageTelemetry.Measurement;
import ai.floedb.floecat.telemetry.StorageTelemetry.Operation;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.TestObservability;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollector;

/** Contract tests for translating AWS SDK metric trees into bounded storage measurements. */
class AwsStorageMetricPublisherTest {
  @Test
  void logicalFakeAndSdkCallCountsRemainIndependentlyReconcilable() {
    TestObservability observability = new TestObservability();
    StorageTelemetry telemetry = new StorageTelemetry(observability);
    AwsStorageMetricPublisher publisher = new AwsStorageMetricPublisher(telemetry);
    AtomicInteger fakeCalls = new AtomicInteger();
    telemetry.observe(
        new Call(Backend.DYNAMODB, Operation.GET),
        () -> {
          fakeCalls.incrementAndGet();
          return "value";
        },
        ignored -> Measurement.of(5L, 1L));
    MetricCollector sdkCall = MetricCollector.create("ApiCall");
    sdkCall.reportMetric(CoreMetric.SERVICE_ID, "DynamoDb");
    sdkCall.reportMetric(CoreMetric.OPERATION_NAME, "GetItem");
    sdkCall.reportMetric(CoreMetric.API_CALL_SUCCESSFUL, true);

    publisher.publish(sdkCall.collect());

    assertThat(fakeCalls).hasValue(1);
    assertThat(observability.scopes().get("STORE"))
        .singleElement()
        .satisfies(scope -> assertThat(scope.operation()).isEqualTo("get"));
    assertThat(observability.counterTagHistory(Telemetry.Metrics.STORE_REQUESTS))
        .extracting(
            tags ->
                tags.stream()
                    .filter(tag -> tag.key().equals("operation"))
                    .findFirst()
                    .orElseThrow()
                    .value())
        .containsExactly("get", "get_item");
  }

  @Test
  void publishesBoundedCallRetryAndAttemptTimings() {
    TestObservability observability = new TestObservability();
    AwsStorageMetricPublisher publisher =
        new AwsStorageMetricPublisher(new StorageTelemetry(observability));
    MetricCollector root = MetricCollector.create("ApiCall");
    root.reportMetric(CoreMetric.SERVICE_ID, "DynamoDb");
    root.reportMetric(CoreMetric.OPERATION_NAME, "GetItem");
    root.reportMetric(CoreMetric.API_CALL_SUCCESSFUL, true);
    root.reportMetric(CoreMetric.RETRY_COUNT, 2);
    root.reportMetric(CoreMetric.API_CALL_DURATION, Duration.ofMillis(4));
    MetricCollector attempt = root.createChild("ApiCallAttempt");
    attempt.reportMetric(CoreMetric.SERVICE_CALL_DURATION, Duration.ofMillis(3));
    attempt.reportMetric(CoreMetric.MARSHALLING_DURATION, Duration.ofNanos(200_000));
    attempt.reportMetric(HttpMetric.CONCURRENCY_ACQUIRE_DURATION, Duration.ofNanos(100_000));
    MetricCollector retryAttempt = root.createChild("ApiCallAttempt");
    retryAttempt.reportMetric(CoreMetric.SERVICE_CALL_DURATION, Duration.ofMillis(7));

    publisher.publish(root.collect());

    assertThat(observability.counterTagHistory(Telemetry.Metrics.STORE_REQUESTS))
        .singleElement()
        .satisfies(
            tags ->
                assertThat(tags)
                    .contains(
                        Tag.of("component", "aws_sdk_dynamodb"),
                        Tag.of("operation", "get_item"),
                        Tag.of("result", "success")));
    assertThat(observability.counterValue(Telemetry.Metrics.STORE_RETRIES)).isEqualTo(2d);
    assertThat(observability.timerTagHistory(Telemetry.Metrics.STORE_LATENCY))
        .extracting(
            tags ->
                tags.stream()
                    .filter(tag -> tag.key().equals("operation"))
                    .findFirst()
                    .orElseThrow()
                    .value())
        .containsExactlyInAnyOrder(
            "get_item.api_call",
            "get_item.service_call",
            "get_item.service_call",
            "get_item.marshalling",
            "get_item.concurrency_acquire");
    assertThat(observability.timerValues(Telemetry.Metrics.STORE_LATENCY))
        .contains(Duration.ofMillis(3), Duration.ofMillis(7))
        .doesNotContain(Duration.ofMillis(10));
  }

  @Test
  void ignoresUnknownServicesAndToleratesMissingOptionalMetrics() {
    TestObservability observability = new TestObservability();
    AwsStorageMetricPublisher publisher =
        new AwsStorageMetricPublisher(new StorageTelemetry(observability));
    MetricCollector unknown = MetricCollector.create("ApiCall");
    unknown.reportMetric(CoreMetric.SERVICE_ID, "SecretsManager");
    unknown.reportMetric(CoreMetric.OPERATION_NAME, "GetSecretValue");

    publisher.publish(unknown.collect());
    publisher.close();
    publisher.close();

    assertThat(observability.counterValue(Telemetry.Metrics.STORE_REQUESTS)).isZero();
  }

  @Test
  void sharedPublisherContinuesAfterClientCloseAndRecordsFailureWithoutDurations() {
    TestObservability observability = new TestObservability();
    AwsStorageMetricPublisher publisher =
        new AwsStorageMetricPublisher(new StorageTelemetry(observability));
    publisher.close();
    MetricCollector failed = MetricCollector.create("ApiCall");
    failed.reportMetric(CoreMetric.SERVICE_ID, "S3");
    failed.reportMetric(CoreMetric.OPERATION_NAME, "GetObject");
    failed.reportMetric(CoreMetric.API_CALL_SUCCESSFUL, false);

    publisher.publish(failed.collect());

    assertThat(observability.counterTagHistory(Telemetry.Metrics.STORE_REQUESTS))
        .singleElement()
        .satisfies(
            tags ->
                assertThat(tags)
                    .contains(
                        Tag.of("component", "aws_sdk_s3"),
                        Tag.of("operation", "get_object"),
                        Tag.of("result", "error")));
  }
}

/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package ai.floedb.floecat.service.metagraph.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.util.Timestamps;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class SnapshotRetentionPolicyTest {

  private static final Instant NOW = Instant.parse("2026-01-31T00:00:00Z");

  private SnapshotRetentionPolicy policy() {
    return new SnapshotRetentionPolicy(
        Clock.fixed(NOW, java.time.ZoneOffset.UTC), Duration.ofDays(30), Duration.ofDays(7));
  }

  @Test
  void visibilityUsesFloecatPublicationTime() {
    var policy = policy();
    assertThat(policy.visible(Timestamps.fromMillis(NOW.minus(Duration.ofDays(30)).toEpochMilli())))
        .isTrue();
    assertThat(
            policy.visible(
                Timestamps.fromMillis(
                    NOW.minus(Duration.ofDays(30)).minusMillis(1).toEpochMilli())))
        .isFalse();
  }

  @Test
  void gcRequiresRetentionAndGrace() {
    var policy = policy();
    assertThat(
            policy.gcEligible(Timestamps.fromMillis(NOW.minus(Duration.ofDays(37)).toEpochMilli())))
        .isFalse();
    assertThat(
            policy.gcEligible(
                Timestamps.fromMillis(
                    NOW.minus(Duration.ofDays(37)).minusMillis(1).toEpochMilli())))
        .isTrue();
  }

  @Test
  void missingPublicationTimeIsRetained() {
    var policy = policy();
    assertThat(policy.visible(null)).isTrue();
    assertThat(policy.gcEligible(null)).isFalse();
    assertThat(policy.visible(com.google.protobuf.Timestamp.getDefaultInstance())).isTrue();
    assertThat(policy.gcEligible(com.google.protobuf.Timestamp.getDefaultInstance())).isFalse();
  }

  @Test
  void zeroRetentionDisablesExpiryUntilConfigured() {
    var policy =
        new SnapshotRetentionPolicy(
            Clock.fixed(NOW, java.time.ZoneOffset.UTC), Duration.ZERO, Duration.ofDays(7));
    assertThat(policy.visible(Timestamps.fromMillis(0))).isTrue();
    assertThat(policy.gcEligible(Timestamps.fromMillis(0))).isFalse();
  }

  @Test
  void negativeDurationsAreRejected() {
    assertThatThrownBy(
            () ->
                new SnapshotRetentionPolicy(Clock.systemUTC(), Duration.ofDays(-1), Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class);
  }
}

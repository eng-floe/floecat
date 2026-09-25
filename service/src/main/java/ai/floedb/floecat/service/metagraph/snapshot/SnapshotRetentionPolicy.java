/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package ai.floedb.floecat.service.metagraph.snapshot;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** The one place where snapshot visibility and GC retention cutoffs are defined. */
@ApplicationScoped
public final class SnapshotRetentionPolicy {

  private final Clock clock;
  private final Duration retention;
  private final Duration grace;

  public SnapshotRetentionPolicy(
      @ConfigProperty(name = "floecat.snapshot.retention") Duration retention,
      @ConfigProperty(name = "floecat.snapshot.retention-grace") Duration grace) {
    this(Clock.systemUTC(), retention, grace);
  }

  public SnapshotRetentionPolicy(Clock clock, Duration retention, Duration grace) {
    if (retention.isNegative() || grace.isNegative()) {
      throw new IllegalArgumentException("snapshot retention and grace must be non-negative");
    }
    this.clock = clock;
    this.retention = retention;
    this.grace = grace;
  }

  public Instant visibilityCutoff() {
    return clock.instant().minus(retention);
  }

  public Instant gcCutoff() {
    return clock.instant().minus(retention).minus(grace);
  }

  /** Missing publication metadata is retained conservatively until it can be backfilled. */
  public boolean visible(Timestamp publishedAt) {
    if (retention.isZero()) {
      return true;
    }
    return publishedAt == null
        || publishedAt.getSeconds() == 0 && publishedAt.getNanos() == 0
        || !publishedAtBefore(publishedAt, visibilityCutoff());
  }

  /** Missing publication metadata is never eligible for deletion. */
  public boolean gcEligible(Timestamp publishedAt) {
    if (retention.isZero()) {
      return false;
    }
    return publishedAt != null
        && !(publishedAt.getSeconds() == 0 && publishedAt.getNanos() == 0)
        && publishedAtBefore(publishedAt, gcCutoff());
  }

  private static boolean publishedAtBefore(Timestamp publishedAt, Instant cutoff) {
    return Timestamps.toMillis(publishedAt) < cutoff.toEpochMilli();
  }

  Duration retention() {
    return retention;
  }

  Duration grace() {
    return grace;
  }
}

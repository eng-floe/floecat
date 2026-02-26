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

package ai.floedb.floecat.types;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;

/** Shared helpers for timestamp/time coercion across stats, encoders, and comparators. */
public final class TemporalCoercions {
  public static final long NANOS_PER_DAY = 86_400_000_000_000L;

  private static final long TIME_SECONDS_THRESHOLD = 86_400L;
  private static final long TIME_MILLIS_THRESHOLD = 86_400_000L;
  private static final long TIME_MICROS_THRESHOLD = 86_400_000_000L;

  private static final long NANOS_THRESHOLD = 1_000_000_000_000_000_000L;
  private static final long MICROS_THRESHOLD = 1_000_000_000_000_000L;
  private static final long MILLIS_THRESHOLD = 1_000_000_000_000L;

  private TemporalCoercions() {}

  public static long timeNanosOfDay(long v) {
    long nanos = toTimeNanos(v);
    return Math.floorMod(nanos, NANOS_PER_DAY);
  }

  public static Instant instantFromNumber(long v) {
    long av = Math.abs(v);
    if (av >= NANOS_THRESHOLD) {
      long secs = Math.floorDiv(v, 1_000_000_000L);
      long nanos = Math.floorMod(v, 1_000_000_000L);
      return truncateToMicros(Instant.ofEpochSecond(secs, nanos));
    }
    if (av >= MICROS_THRESHOLD) {
      long secs = Math.floorDiv(v, 1_000_000L);
      long micros = Math.floorMod(v, 1_000_000L);
      return truncateToMicros(Instant.ofEpochSecond(secs, micros * 1_000L));
    }
    if (av >= MILLIS_THRESHOLD) {
      return truncateToMicros(Instant.ofEpochMilli(v));
    }
    return truncateToMicros(Instant.ofEpochSecond(v));
  }

  public static LocalDateTime localDateTimeFromNumber(long v) {
    return truncateToMicros(LocalDateTime.ofInstant(instantFromNumber(v), ZoneOffset.UTC));
  }

  public enum TimestampNoTzPolicy {
    REJECT_ZONED,
    CONVERT_TO_SESSION_ZONE
  }

  public static LocalDateTime parseTimestampNoTz(String raw) {
    return parseTimestampNoTz(raw, defaultSessionZone(), defaultTimestampNoTzPolicy());
  }

  public static LocalDateTime parseTimestampNoTz(
      String raw, ZoneId sessionZone, TimestampNoTzPolicy policy) {
    try {
      return truncateToMicros(LocalDateTime.parse(raw, DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    } catch (DateTimeParseException ignore) {
      // fall through
    }

    Instant instant = parseZonedInstant(raw);
    if (policy == TimestampNoTzPolicy.REJECT_ZONED) {
      throw new IllegalArgumentException(
          "TIMESTAMP must be timezone-naive, got zoned value: " + raw);
    }
    ZoneId zone = (sessionZone == null) ? ZoneOffset.UTC : sessionZone;
    return truncateToMicros(LocalDateTime.ofInstant(instant, zone));
  }

  public static Instant parseZonedInstant(String raw) {
    try {
      return Instant.parse(raw);
    } catch (DateTimeParseException ignore) {
      return OffsetDateTime.parse(raw).toInstant();
    }
  }

  public static ZoneId defaultSessionZone() {
    String zone = System.getProperty("floecat.session.timezone");
    if (zone == null || zone.isBlank()) {
      zone = System.getenv("FLOECAT_SESSION_TIMEZONE");
    }
    if (zone == null || zone.isBlank()) {
      return ZoneOffset.UTC;
    }
    return ZoneId.of(zone);
  }

  public static TimestampNoTzPolicy defaultTimestampNoTzPolicy() {
    String policy = System.getProperty("floecat.timestamp_no_tz.policy");
    if (policy == null || policy.isBlank()) {
      policy = System.getenv("FLOECAT_TIMESTAMP_NO_TZ_POLICY");
    }
    if (policy == null || policy.isBlank()) {
      return TimestampNoTzPolicy.REJECT_ZONED;
    }
    String normalized = policy.trim().toUpperCase(Locale.ROOT);
    return switch (normalized) {
      case "REJECT", "REJECT_ZONED" -> TimestampNoTzPolicy.REJECT_ZONED;
      case "CONVERT", "CONVERT_TO_SESSION_ZONE" -> TimestampNoTzPolicy.CONVERT_TO_SESSION_ZONE;
      default ->
          throw new IllegalArgumentException("Invalid floecat.timestamp_no_tz.policy: " + policy);
    };
  }

  public static LocalTime truncateToMicros(LocalTime time) {
    int nanos = time.getNano();
    if ((nanos % 1_000) == 0) {
      return time;
    }
    return time.withNano((nanos / 1_000) * 1_000);
  }

  public static LocalDateTime truncateToMicros(LocalDateTime time) {
    int nanos = time.getNano();
    if ((nanos % 1_000) == 0) {
      return time;
    }
    return time.withNano((nanos / 1_000) * 1_000);
  }

  public static Instant truncateToMicros(Instant instant) {
    long nanos = instant.getNano();
    if ((nanos % 1_000) == 0) {
      return instant;
    }
    long truncated = (nanos / 1_000) * 1_000;
    return Instant.ofEpochSecond(instant.getEpochSecond(), truncated);
  }

  private static long toTimeNanos(long v) {
    long abs = Math.abs(v);
    if (abs <= TIME_SECONDS_THRESHOLD) {
      return v * 1_000_000_000L; // seconds
    }
    if (abs <= TIME_MILLIS_THRESHOLD) {
      return v * 1_000_000L; // milliseconds
    }
    if (abs <= TIME_MICROS_THRESHOLD) {
      return v * 1_000L; // microseconds
    }
    return v; // nanoseconds
  }
}

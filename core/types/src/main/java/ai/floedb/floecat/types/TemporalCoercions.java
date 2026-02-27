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

  private static final int[] POW10 =
      new int[] {
        1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000
      };

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

  public static LocalTime truncateToTemporalPrecision(LocalTime time, Integer precision) {
    int p = normalizeTemporalPrecision(precision);
    return truncateToTemporalPrecision(time, p);
  }

  public static LocalDateTime truncateToTemporalPrecision(LocalDateTime time, Integer precision) {
    int p = normalizeTemporalPrecision(precision);
    return truncateToTemporalPrecision(time, p);
  }

  public static Instant truncateToTemporalPrecision(Instant instant, Integer precision) {
    int p = normalizeTemporalPrecision(precision);
    return truncateToTemporalPrecision(instant, p);
  }

  private static LocalTime truncateToTemporalPrecision(LocalTime time, int precision) {
    int nanos = time.getNano();
    int truncated = truncateNanos(nanos, precision);
    if (truncated == nanos) {
      return time;
    }
    return time.withNano(truncated);
  }

  private static LocalDateTime truncateToTemporalPrecision(LocalDateTime time, int precision) {
    int nanos = time.getNano();
    int truncated = truncateNanos(nanos, precision);
    if (truncated == nanos) {
      return time;
    }
    return time.withNano(truncated);
  }

  private static Instant truncateToTemporalPrecision(Instant instant, int precision) {
    int nanos = instant.getNano();
    int truncated = truncateNanos(nanos, precision);
    if (truncated == nanos) {
      return instant;
    }
    return Instant.ofEpochSecond(instant.getEpochSecond(), truncated);
  }

  public static String formatLocalTime(LocalTime time, Integer precision) {
    if (precision == null) {
      return DateTimeFormatter.ISO_LOCAL_TIME.format(truncateToMicros(time));
    }
    int p = normalizeTemporalPrecision(precision);
    LocalTime truncated = truncateToTemporalPrecision(time, p);
    String base =
        String.format(
            Locale.ROOT,
            "%02d:%02d:%02d",
            truncated.getHour(),
            truncated.getMinute(),
            truncated.getSecond());
    if (p == 0) {
      return base;
    }
    int frac = truncated.getNano() / POW10[9 - p];
    return base + "." + String.format(Locale.ROOT, "%0" + p + "d", frac);
  }

  public static String formatLocalDateTime(LocalDateTime time, Integer precision) {
    if (precision == null) {
      return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(truncateToMicros(time));
    }
    int p = normalizeTemporalPrecision(precision);
    LocalDateTime truncated = truncateToTemporalPrecision(time, p);
    String base =
        DateTimeFormatter.ISO_LOCAL_DATE.format(truncated.toLocalDate())
            + "T"
            + formatLocalTime(truncated.toLocalTime(), p);
    return base;
  }

  public static String formatInstantUtc(Instant instant, Integer precision) {
    if (precision == null) {
      return DateTimeFormatter.ISO_INSTANT.format(truncateToMicros(instant));
    }
    int p = normalizeTemporalPrecision(precision);
    Instant truncated = truncateToTemporalPrecision(instant, p);
    LocalDateTime local = LocalDateTime.ofInstant(truncated, ZoneOffset.UTC);
    return formatLocalDateTime(local, p) + "Z";
  }

  private static int normalizeTemporalPrecision(Integer precision) {
    if (precision == null) {
      return LogicalType.DEFAULT_TEMPORAL_PRECISION;
    }
    if (precision < 0 || precision > LogicalType.MAX_TEMPORAL_PRECISION) {
      throw new IllegalArgumentException(
          "Invalid temporal precision: "
              + precision
              + " (must be between 0 and "
              + LogicalType.MAX_TEMPORAL_PRECISION
              + ")");
    }
    return precision;
  }

  private static int truncateNanos(int nanos, int precision) {
    int factor = POW10[9 - precision];
    return (nanos / factor) * factor;
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

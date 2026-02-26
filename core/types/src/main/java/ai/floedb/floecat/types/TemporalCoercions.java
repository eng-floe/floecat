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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/** Shared helpers for timestamp/time coercion across stats, encoders, and comparators. */
final class TemporalCoercions {
  static final long NANOS_PER_DAY = 86_400_000_000_000L;

  private static final long TIME_SECONDS_THRESHOLD = 86_400L;
  private static final long TIME_MILLIS_THRESHOLD = 86_400_000L;
  private static final long TIME_MICROS_THRESHOLD = 86_400_000_000L;

  private static final long NANOS_THRESHOLD = 1_000_000_000_000_000_000L;
  private static final long MICROS_THRESHOLD = 1_000_000_000_000_000L;
  private static final long MILLIS_THRESHOLD = 1_000_000_000_000L;

  private TemporalCoercions() {}

  static long timeNanosOfDay(long v) {
    long nanos = toTimeNanos(v);
    return Math.floorMod(nanos, NANOS_PER_DAY);
  }

  static Instant instantFromNumber(long v) {
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

  static LocalDateTime localDateTimeFromNumber(long v) {
    return truncateToMicros(LocalDateTime.ofInstant(instantFromNumber(v), ZoneOffset.UTC));
  }

  static LocalDateTime parseTimestampNoTz(String raw) {
    try {
      return truncateToMicros(LocalDateTime.parse(raw, DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    } catch (DateTimeParseException ignore) {
      return truncateToMicros(LocalDateTime.ofInstant(Instant.parse(raw), ZoneOffset.UTC));
    }
  }

  static LocalTime truncateToMicros(LocalTime time) {
    int nanos = time.getNano();
    if ((nanos % 1_000) == 0) {
      return time;
    }
    return time.withNano((nanos / 1_000) * 1_000);
  }

  static LocalDateTime truncateToMicros(LocalDateTime time) {
    int nanos = time.getNano();
    if ((nanos % 1_000) == 0) {
      return time;
    }
    return time.withNano((nanos / 1_000) * 1_000);
  }

  static Instant truncateToMicros(Instant instant) {
    long nanos = instant.getNano();
    if ((nanos % 1_000) == 0) {
      return instant;
    }
    long truncated = (nanos / 1_000) * 1_000;
    return Instant.ofEpochSecond(instant.getEpochSecond(), truncated);
  }

  private static long toTimeNanos(long v) {
    long abs = Math.abs(v);
    if (abs < TIME_SECONDS_THRESHOLD) {
      return v * 1_000_000_000L; // seconds
    }
    if (abs < TIME_MILLIS_THRESHOLD) {
      return v * 1_000_000L; // milliseconds
    }
    if (abs < TIME_MICROS_THRESHOLD) {
      return v * 1_000L; // microseconds
    }
    return v; // nanoseconds
  }
}

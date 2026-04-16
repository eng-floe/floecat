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

package ai.floedb.floecat.arrow;

import com.google.protobuf.Timestamp;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;

/** Lightweight helpers for writing values into Arrow vectors. */
public final class ArrowValueWriters {

  private ArrowValueWriters() {}

  public static void writeVarChar(VarCharVector vector, int index, String value) {
    Objects.requireNonNull(vector, "vector");
    if (value == null) {
      vector.setNull(index);
      return;
    }
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    vector.setSafe(index, bytes);
  }

  public static void writeBigInt(BigIntVector vector, int index, long value) {
    Objects.requireNonNull(vector, "vector");
    vector.setSafe(index, value);
  }

  public static void writeBigIntNullable(BigIntVector vector, int index, OptionalLong value) {
    Objects.requireNonNull(vector, "vector");
    Objects.requireNonNull(value, "value");
    if (value.isPresent()) {
      vector.setSafe(index, value.getAsLong());
      return;
    }
    vector.setNull(index);
  }

  public static void writeDouble(Float8Vector vector, int index, double value) {
    Objects.requireNonNull(vector, "vector");
    vector.setSafe(index, value);
  }

  public static void writeDoubleNullable(Float8Vector vector, int index, OptionalDouble value) {
    Objects.requireNonNull(vector, "vector");
    Objects.requireNonNull(value, "value");
    if (value.isPresent()) {
      vector.setSafe(index, value.getAsDouble());
      return;
    }
    vector.setNull(index);
  }

  public static void writeTimestamp(TimeStampMicroTZVector vector, int index, Timestamp value) {
    Objects.requireNonNull(vector, "vector");
    if (value == null) {
      vector.setNull(index);
      return;
    }
    long micros = value.getSeconds() * 1_000_000L + (value.getNanos() / 1_000L);
    vector.setSafe(index, micros);
  }
}

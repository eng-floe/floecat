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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Timestamp;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.Test;

class ArrowValueWritersTest {

  @Test
  void writesVarCharWithUtf8AndNull() {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VarCharVector vector = new VarCharVector("v", allocator)) {
      vector.allocateNew();
      ArrowValueWriters.writeVarChar(vector, 0, "alpha");
      ArrowValueWriters.writeVarChar(vector, 1, null);

      assertThat(vector.getObject(0).toString()).isEqualTo("alpha");
      assertThat(vector.isNull(1)).isTrue();
    }
  }

  @Test
  void writesBigIntAndNullableBigInt() {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        BigIntVector vector = new BigIntVector("v", allocator)) {
      vector.allocateNew();
      ArrowValueWriters.writeBigInt(vector, 0, 123L);
      ArrowValueWriters.writeBigIntNullable(vector, 1, OptionalLong.of(456L));
      ArrowValueWriters.writeBigIntNullable(vector, 2, OptionalLong.empty());

      assertThat(vector.get(0)).isEqualTo(123L);
      assertThat(vector.get(1)).isEqualTo(456L);
      assertThat(vector.isNull(2)).isTrue();
    }
  }

  @Test
  void writesDoubleAndNullableDouble() {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Float8Vector vector = new Float8Vector("v", allocator)) {
      vector.allocateNew();
      ArrowValueWriters.writeDouble(vector, 0, 3.5);
      ArrowValueWriters.writeDoubleNullable(vector, 1, OptionalDouble.of(7.25));
      ArrowValueWriters.writeDoubleNullable(vector, 2, OptionalDouble.empty());

      assertThat(vector.get(0)).isEqualTo(3.5);
      assertThat(vector.get(1)).isEqualTo(7.25);
      assertThat(vector.isNull(2)).isTrue();
    }
  }

  @Test
  void writesTimestampMicrosAndNull() {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        TimeStampMicroTZVector vector = new TimeStampMicroTZVector("v", allocator, "UTC")) {
      vector.allocateNew();
      Timestamp ts = Timestamp.newBuilder().setSeconds(10L).setNanos(123_456_000).build();
      ArrowValueWriters.writeTimestamp(vector, 0, ts);
      ArrowValueWriters.writeTimestamp(vector, 1, null);

      assertThat(vector.get(0)).isEqualTo(10_123_456L);
      assertThat(vector.isNull(1)).isTrue();
    }
  }
}

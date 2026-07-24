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

package ai.floedb.floecat.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.reconciler.rpc.FileGroupStatsPayload;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class ProtobufMessageRangesTest {
  @Test
  void locatesExactRepeatedMessagePayloadBytes() throws Exception {
    TargetStatsRecord first = TargetStatsRecord.newBuilder().setSnapshotId(10L).build();
    TargetStatsRecord second = TargetStatsRecord.newBuilder().setSnapshotId(20L).build();
    byte[] payload =
        FileGroupStatsPayload.newBuilder()
            .setFormatVersion(1)
            .setAccountId("acct")
            .addFileStats(first)
            .addFileStats(second)
            .build()
            .toByteArray();

    var ranges = ProtobufMessageRanges.locate(payload, 12);

    assertEquals(2, ranges.size());
    assertEquals(first, decode(payload, ranges.get(0)));
    assertEquals(second, decode(payload, ranges.get(1)));
  }

  private static TargetStatsRecord decode(byte[] payload, ProtobufMessageRanges.ByteRange range)
      throws Exception {
    int start = Math.toIntExact(range.offset());
    return TargetStatsRecord.parseFrom(Arrays.copyOfRange(payload, start, start + range.length()));
  }
}

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

package ai.floedb.floecat.reconciler.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import com.google.protobuf.ByteString;
import java.util.List;
import org.junit.jupiter.api.Test;

class ArtifactReferenceDigestTest {

  @Test
  void descriptorOrderUsesUnsignedUtf8BytesAcrossUnicodePlanes() {
    StatsObjectDescriptor bmp = descriptor("file:\uE000", "uri-a");
    StatsObjectDescriptor supplementary = descriptor("file:\uD800\uDC00", "uri-b");

    assertEquals(
        "ebd01667a922b57cb945742d6ff78296f0b7b3ec8f66260b25d4db9e9e572cfd",
        ArtifactReferenceDigest.sha256(List.of(supplementary, bmp), List.of()));
  }

  private static StatsObjectDescriptor descriptor(String target, String uri) {
    return StatsObjectDescriptor.newBuilder()
        .setTargetStorageId(target)
        .setPayloadUri(uri)
        .setPayloadBytes(1L)
        .setPayloadSha256(ByteString.copyFrom(new byte[32]))
        .build();
  }
}

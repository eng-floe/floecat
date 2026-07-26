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

import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;

/** Canonical metadata-only digest for a file group's externally written artifact references. */
public final class ArtifactReferenceDigest {
  private ArtifactReferenceDigest() {}

  public static String sha256(
      List<StatsObjectDescriptor> fileStats, List<StatsObjectDescriptor> indexArtifacts) {
    MessageDigest digest = sha256Digest();
    updateGroup(digest, (byte) 1, fileStats);
    updateGroup(digest, (byte) 2, indexArtifacts);
    return HexFormat.of().formatHex(digest.digest());
  }

  private static void updateGroup(
      MessageDigest digest, byte kind, List<StatsObjectDescriptor> descriptors) {
    List<StatsObjectDescriptor> stable =
        (descriptors == null ? List.<StatsObjectDescriptor>of() : descriptors)
            .stream()
                .filter(java.util.Objects::nonNull)
                .sorted(
                    Comparator.comparing(StatsObjectDescriptor::getTargetStorageId)
                        .thenComparing(StatsObjectDescriptor::getPayloadUri)
                        .thenComparingLong(StatsObjectDescriptor::getPayloadBytes)
                        .thenComparing(
                            value ->
                                HexFormat.of().formatHex(value.getPayloadSha256().toByteArray())))
                .toList();
    digest.update(kind);
    updateInt(digest, stable.size());
    for (StatsObjectDescriptor descriptor : stable) {
      updateBytes(digest, descriptor.getTargetStorageId().getBytes(StandardCharsets.UTF_8));
      updateBytes(digest, descriptor.getPayloadUri().getBytes(StandardCharsets.UTF_8));
      digest.update(ByteBuffer.allocate(Long.BYTES).putLong(descriptor.getPayloadBytes()).array());
      updateBytes(digest, descriptor.getPayloadSha256().toByteArray());
    }
  }

  private static void updateBytes(MessageDigest digest, byte[] bytes) {
    updateInt(digest, bytes.length);
    digest.update(bytes);
  }

  private static void updateInt(MessageDigest digest, int value) {
    digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
  }

  private static MessageDigest sha256Digest() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }
}

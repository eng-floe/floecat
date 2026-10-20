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

package ai.floedb.floecat.schema.identity;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/** Defines the disjoint native, allocated, and collection-derived canonical ID spaces. */
final class CanonicalColumnId {
  static final long DERIVED_NAMESPACE_BIT = 1L << 62;
  static final long MAX_ALLOCATED_ID = DERIVED_NAMESPACE_BIT - 1L;
  private static final long DERIVED_PAYLOAD_MASK = DERIVED_NAMESPACE_BIT - 1L;
  private static final byte[] COLLECTION_ID_DOMAIN =
      "floecat.collection-column-id.v2".getBytes(StandardCharsets.UTF_8);

  private CanonicalColumnId() {}

  static void checkAllocatedRange(long highWaterMark) {
    if (highWaterMark < 0L || highWaterMark > MAX_ALLOCATED_ID) {
      throw new IllegalArgumentException(
          "High-water mark must be in the allocated canonical ID space");
    }
  }

  static int nativeFieldId(SchemaNode node) {
    OptionalInt nativeId = node.nativeFieldId();
    if (nativeId.isEmpty() || nativeId.getAsInt() <= 0) {
      throw new IllegalArgumentException(
          "Mapped field " + node.path().display() + " has no positive native field ID");
    }
    return nativeId.getAsInt();
  }

  static long collectionInteriorId(SchemaNode node, Map<ColumnPath, Long> canonicalIdsByPath) {
    if (node.kind() == NodeKind.FIELD) {
      throw new IllegalArgumentException("A field cannot use a derived collection identity");
    }

    List<ColumnPath.Element> elements = node.path().elements();
    int ancestorIndex = elements.size() - 1;
    while (ancestorIndex >= 0 && elements.get(ancestorIndex).kind() != NodeKind.FIELD) {
      ancestorIndex--;
    }
    if (ancestorIndex < 0) {
      throw new IllegalArgumentException(
          "Collection interior " + node.path().display() + " has no ancestor field");
    }

    ColumnPath ancestorPath = new ColumnPath(elements.subList(0, ancestorIndex + 1));
    Long ancestorCanonicalId = canonicalIdsByPath.get(ancestorPath);
    if (ancestorCanonicalId == null) {
      throw new IllegalArgumentException(
          "Collection interior "
              + node.path().display()
              + " has no canonical ancestor field "
              + ancestorPath.display());
    }
    return hashedCollectionInteriorId(ancestorCanonicalId, elements, ancestorIndex + 1);
  }

  /**
   * The hash input is deliberately language-neutral: an ASCII domain, a big-endian signed 64-bit
   * canonical ancestor ID, a big-endian signed 32-bit suffix length, and one stable node-kind byte
   * per suffix element. The first big-endian 64 bits of SHA-256 supply the 62-bit payload.
   */
  private static long hashedCollectionInteriorId(
      long ancestorCanonicalId, List<ColumnPath.Element> elements, int suffixStart) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(COLLECTION_ID_DOMAIN);
      digest.update(ByteBuffer.allocate(Long.BYTES).putLong(ancestorCanonicalId).array());
      digest.update(
          ByteBuffer.allocate(Integer.BYTES).putInt(elements.size() - suffixStart).array());
      for (int i = suffixStart; i < elements.size(); i++) {
        digest.update((byte) elements.get(i).kind().stableCode());
      }
      long payload = ByteBuffer.wrap(digest.digest()).getLong() & DERIVED_PAYLOAD_MASK;
      return DERIVED_NAMESPACE_BIT | payload;
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  static boolean isDerived(long canonicalId) {
    return (canonicalId & DERIVED_NAMESPACE_BIT) != 0L;
  }
}

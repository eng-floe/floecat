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

import java.util.List;
import java.util.OptionalInt;

/** Defines the disjoint native, allocated, and collection-derived canonical ID spaces. */
final class CanonicalColumnId {
  static final long DERIVED_NAMESPACE_BIT = 1L << 62;
  static final long MAX_ALLOCATED_ID = DERIVED_NAMESPACE_BIT - 1L;
  static final int MAX_COLLECTION_DEPTH = 12;

  private static final int SUFFIX_BITS = 24;
  private static final long SUFFIX_MASK = (1L << SUFFIX_BITS) - 1L;
  private static final long ANCESTOR_MASK = ((1L << 31) - 1L) << SUFFIX_BITS;
  private static final long DERIVED_ID_MASK = DERIVED_NAMESPACE_BIT | ANCESTOR_MASK | SUFFIX_MASK;

  private CanonicalColumnId() {}

  static void checkAllocatedRange(long highWaterMark) {
    if (highWaterMark < 0L || highWaterMark > MAX_ALLOCATED_ID) {
      throw new IllegalArgumentException(
          "High-water mark must be in the allocated canonical ID space");
    }
  }

  static long nativeFieldId(SchemaNode node) {
    OptionalInt nativeId = node.nativeFieldId();
    if (nativeId.isEmpty() || nativeId.getAsInt() <= 0) {
      throw new IllegalArgumentException(
          "Mapped field " + node.path().display() + " has no positive native field ID");
    }
    return nativeId.getAsInt();
  }

  static long collectionInteriorId(ResolvedSchema schema, SchemaNode node) {
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

    int depth = elements.size() - ancestorIndex - 1;
    if (depth > MAX_COLLECTION_DEPTH) {
      throw new IllegalArgumentException(
          "Collection interior "
              + node.path().display()
              + " exceeds the maximum derived depth of "
              + MAX_COLLECTION_DEPTH);
    }

    ColumnPath ancestorPath = new ColumnPath(elements.subList(0, ancestorIndex + 1));
    SchemaNode ancestor =
        schema
            .byPath(ancestorPath)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Collection interior "
                            + node.path().display()
                            + " has no resolved ancestor field "
                            + ancestorPath.display()));
    long ancestorNativeId = nativeFieldId(ancestor);
    long suffixCode = 0L;
    for (int i = ancestorIndex + 1; i < elements.size(); i++) {
      suffixCode = (suffixCode << 2) | elements.get(i).kind().collectionSuffixDigit();
    }
    return DERIVED_NAMESPACE_BIT | (ancestorNativeId << SUFFIX_BITS) | suffixCode;
  }

  static boolean isDerived(long canonicalId) {
    return (canonicalId & DERIVED_NAMESPACE_BIT) != 0L;
  }

  static boolean isWellFormedDerived(long canonicalId) {
    if (canonicalId <= 0L
        || !isDerived(canonicalId)
        || (canonicalId & ~DERIVED_ID_MASK) != 0L
        || ((canonicalId & ANCESTOR_MASK) >>> SUFFIX_BITS) == 0L) {
      return false;
    }
    long suffix = canonicalId & SUFFIX_MASK;
    if (suffix == 0L) {
      return false;
    }
    while (suffix != 0L) {
      if ((suffix & 3L) == 0L) {
        return false;
      }
      suffix >>>= 2;
    }
    return true;
  }
}

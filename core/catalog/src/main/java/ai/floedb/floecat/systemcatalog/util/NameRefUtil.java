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

package ai.floedb.floecat.systemcatalog.util;

import ai.floedb.floecat.common.rpc.NameRef;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class NameRefUtil {
  private NameRefUtil() {}

  /**
   * Returns a canonical representation of a NameRef: path1.path2.name
   *
   * <p>Catalog is intentionally omitted to remain engine-neutral.
   *
   * <p>Folds case. This is an identity key: it backs builtin ResourceIds and the signatures that
   * merge or override builtin definitions, where the two sides are both definitions. To match a
   * name a caller supplied, use {@link #lookupKey(NameRef)}.
   */
  public static String identityKey(NameRef ref) {
    if (ref == null) return "";

    String name = ref.getName().trim().toLowerCase(java.util.Locale.ROOT);
    var path = ref.getPathList();

    if (path.isEmpty()) {
      return name;
    }
    return (String.join(".", path).toLowerCase(java.util.Locale.ROOT) + "." + name);
  }

  /**
   * Returns the key a relation or namespace is matched on: path1.path2.name, spelling preserved.
   *
   * <p>A user relation is keyed by its exact stored name, and a builtin is keyed the same way, so
   * one spelling resolves and `orders` and `ORDERS` are different names.
   */
  public static String matchKey(NameRef ref) {
    if (ref == null) return "";

    String name = ref.getName().trim();
    var path = ref.getPathList();

    if (path.isEmpty()) {
      return name;
    }
    return String.join(".", path) + "." + name;
  }

  public static NameRef fromCanonical(String canonical) {
    String[] parts = canonical.split("\\.");
    if (parts.length == 1) {
      return NameRef.newBuilder().setName(parts[0]).build();
    }
    NameRef.Builder b = NameRef.newBuilder().setName(parts[parts.length - 1]);
    for (int i = 0; i < parts.length - 1; i++) {
      b.addPath(parts[i]);
    }
    return b.build();
  }

  /** Returns the canonical namespace part (path only), or empty if none */
  public static String namespaceCanonical(NameRef ref) {
    if (ref == null || ref.getPathCount() == 0) return "";
    return String.join(".", ref.getPathList()).toLowerCase(java.util.Locale.ROOT);
  }

  public static String namespaceFromCanonical(String canonical) {
    int idx = canonical.lastIndexOf('.');
    return idx < 0 ? "" : canonical.substring(0, idx);
  }

  /** Returns the namespace NameRef (parent path) of a qualified object name. */
  public static Optional<NameRef> namespaceRef(NameRef ref) {
    if (ref == null) {
      return Optional.empty();
    }
    if (ref.getPathCount() == 0) {
      String name = ref.getName() == null ? "" : ref.getName().trim();
      int idx = name.lastIndexOf('.');
      if (idx < 0) {
        return Optional.empty();
      }
      String namespaceCanonical = name.substring(0, idx);
      if (namespaceCanonical.isBlank()) {
        return Optional.empty();
      }
      return Optional.of(fromCanonical(namespaceCanonical));
    }
    NameRef.Builder b = NameRef.newBuilder();
    if (ref.getPathCount() > 1) {
      b.addAllPath(ref.getPathList().subList(0, ref.getPathCount() - 1));
    }
    b.setName(ref.getPath(ref.getPathCount() - 1));
    return Optional.of(b.build());
  }

  /**
   * Creates a NameRef from schema and object name.
   *
   * <p>Schema is the last path segment.
   */
  public static NameRef name(String schema, String table) {
    return NameRef.newBuilder().addPath(schema).setName(table).build();
  }

  /**
   * Creates a NameRef from an arbitrary number of path segments.
   *
   * <p>The last element is treated as the object name; everything before that becomes the path.
   */
  public static NameRef name(String... parts) {
    if (parts == null || parts.length == 0) {
      return NameRef.getDefaultInstance();
    }
    NameRef.Builder b = NameRef.newBuilder().setName(parts[parts.length - 1]);
    for (int i = 0; i < parts.length - 1; i++) {
      b.addPath(parts[i]);
    }
    return b.build();
  }

  /**
   * Builds the effective namespace path from a {@link NameRef}.
   *
   * <p>Matches the path semantics used by fully-qualified resolution: the name is appended to the
   * path if present and not already included.
   */
  public static List<String> namespacePath(NameRef ref) {
    List<String> out = new ArrayList<>(ref.getPathList());
    if (ref.getName() != null && !ref.getName().isBlank()) {
      if (out.isEmpty() || !out.get(out.size() - 1).equals(ref.getName())) {
        out.add(ref.getName());
      }
    }
    return out;
  }

  /**
   * Builds a canonical namespace display form from path segments and leaf display name.
   *
   * <p>The leaf is appended only when not already present as the last path segment.
   */
  public static String namespaceName(List<String> pathSegments, String displayName) {
    return ai.floedb.floecat.scanner.spi.TopologyNames.namespaceName(pathSegments, displayName);
  }
}

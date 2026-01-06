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

public final class NameRefUtil {
  private NameRefUtil() {}

  /**
   * Returns a canonical representation of a NameRef: path1.path2.name
   *
   * <p>Catalog is intentionally omitted to remain engine-neutral.
   *
   * <p>This canonical form lower-cases every segment so it can safely be used as a case-insensitive
   * key when merging or overriding builtin definitions.
   */
  public static String canonical(NameRef ref) {
    if (ref == null) return "";

    String name = ref.getName().trim().toLowerCase();
    var path = ref.getPathList();

    if (path.isEmpty()) {
      return name;
    }
    return (String.join(".", path).toLowerCase() + "." + name);
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
    return String.join(".", ref.getPathList()).toLowerCase();
  }

  public static String namespaceFromCanonical(String canonical) {
    int idx = canonical.lastIndexOf('.');
    return idx < 0 ? "" : canonical.substring(0, idx);
  }

  /**
   * Matches a NameRef against a schema + object identifier.
   *
   * <p>Schema may be nested; we match the last path segment as the schema.
   */
  public static boolean matches(NameRef ref, String schema, String object) {
    if (ref == null || schema == null || object == null) {
      return false;
    }

    // canonical(NameRef) = "a.b.c" or "c"
    String canonical = canonical(ref);

    // expected = schema + "." + object OR object only
    String expected = schema.isEmpty() ? object : (schema + "." + object);

    return canonical.equalsIgnoreCase(expected);
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
}

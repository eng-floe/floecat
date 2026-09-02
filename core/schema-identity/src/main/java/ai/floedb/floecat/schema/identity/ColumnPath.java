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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A structured path to a node in a schema tree, and the canonical identity key for that node.
 *
 * <p>Paths are structured rather than rendered because Delta column names are close to arbitrary
 * strings: a dot, a bracket pair, or the literal text {@code element} inside a name all collide
 * with any dotted rendering. A top-level field named {@code a.b} and a field {@code b} inside a
 * struct {@code a} both render as {@code a.b}, so a rendering cannot serve as an identity key.
 * Equality, hashing, and map lookups therefore run on the element list.
 *
 * <p>{@link #display()} renders the familiar dotted-and-bracketed form for logs, error messages,
 * and the stats selector syntax. It is lossy by construction and must never be parsed back into a
 * path or used to key anything.
 */
public record ColumnPath(List<PathElement> elements) {

  public static final ColumnPath ROOT = new ColumnPath(List.of());

  public ColumnPath {
    elements = List.copyOf(Objects.requireNonNull(elements, "elements"));
  }

  /** One step of a path. {@code name} is set for {@link NodeKind#FIELD} and null otherwise. */
  public record PathElement(NodeKind kind, String name) {

    public static final PathElement ARRAY_ELEMENT = new PathElement(NodeKind.ARRAY_ELEMENT, null);
    public static final PathElement MAP_KEY = new PathElement(NodeKind.MAP_KEY, null);
    public static final PathElement MAP_VALUE = new PathElement(NodeKind.MAP_VALUE, null);

    public PathElement {
      Objects.requireNonNull(kind, "kind");
      if (kind == NodeKind.FIELD) {
        if (name == null || name.isEmpty()) {
          throw new IllegalArgumentException("A FIELD path element requires a name");
        }
      } else if (name != null) {
        throw new IllegalArgumentException(kind + " path elements cannot carry a name");
      }
    }

    public static PathElement field(String name) {
      return new PathElement(NodeKind.FIELD, name);
    }
  }

  public static ColumnPath of(PathElement... elements) {
    return new ColumnPath(List.of(elements));
  }

  public ColumnPath child(PathElement element) {
    List<PathElement> next = new ArrayList<>(elements.size() + 1);
    next.addAll(elements);
    next.add(Objects.requireNonNull(element, "element"));
    return new ColumnPath(next);
  }

  public ColumnPath field(String name) {
    return child(PathElement.field(name));
  }

  public ColumnPath arrayElement() {
    return child(PathElement.ARRAY_ELEMENT);
  }

  public ColumnPath mapKey() {
    return child(PathElement.MAP_KEY);
  }

  public ColumnPath mapValue() {
    return child(PathElement.MAP_VALUE);
  }

  public boolean isRoot() {
    return elements.isEmpty();
  }

  public int depth() {
    return elements.size();
  }

  public PathElement last() {
    if (elements.isEmpty()) {
      throw new IllegalStateException("The root path has no last element");
    }
    return elements.get(elements.size() - 1);
  }

  /**
   * Renders the path for humans: struct children as {@code parent.child}, array elements as {@code
   * parent[]}, map keys as {@code parent.key}, map values as <code>parent{}</code>.
   *
   * <p>Lossy — see the class note. Never use it as a key.
   */
  public String display() {
    StringBuilder out = new StringBuilder();
    for (PathElement element : elements) {
      switch (element.kind()) {
        case FIELD -> {
          if (!out.isEmpty()) {
            out.append('.');
          }
          out.append(element.name());
        }
        case ARRAY_ELEMENT -> out.append("[]");
        case MAP_KEY -> out.append(".key");
        case MAP_VALUE -> out.append("{}");
      }
    }
    return out.toString();
  }

  @Override
  public String toString() {
    return display();
  }
}

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
 * A structured path to a schema node.
 *
 * <p>The path itself is the identity: equality is structural, so a field literally named {@code
 * "a.b"} never equals the nested field {@code a} then {@code b}. Both string renderings below lose
 * that distinction, and neither is ever parsed back into a path.
 *
 * <p>{@link #display()} is for diagnostics. {@link #legacyDottedKey()} is for the string-based
 * connector contract, where callers must guard against collisions with {@link LegacyDottedKeyIndex}
 * rather than assuming the rendered key identifies one column.
 */
public record ColumnPath(List<Element> elements) {

  public static final ColumnPath ROOT = new ColumnPath(List.of());

  public ColumnPath {
    elements = List.copyOf(Objects.requireNonNull(elements, "elements"));
  }

  /** One structural path step. Only fields carry a name. */
  public record Element(NodeKind kind, String name) {
    private static final Element ARRAY_ELEMENT = new Element(NodeKind.ARRAY_ELEMENT, null);
    private static final Element MAP_KEY = new Element(NodeKind.MAP_KEY, null);
    private static final Element MAP_VALUE = new Element(NodeKind.MAP_VALUE, null);

    public Element {
      Objects.requireNonNull(kind, "kind");
      if (kind == NodeKind.FIELD && (name == null || name.isEmpty())) {
        throw new IllegalArgumentException("A field path element requires a name");
      }
      if (kind != NodeKind.FIELD && name != null) {
        throw new IllegalArgumentException(kind + " path elements cannot carry a name");
      }
    }

    private static Element field(String name) {
      return new Element(NodeKind.FIELD, name);
    }
  }

  public ColumnPath field(String name) {
    return child(Element.field(name));
  }

  public ColumnPath arrayElement() {
    return child(Element.ARRAY_ELEMENT);
  }

  public ColumnPath mapKey() {
    return child(Element.MAP_KEY);
  }

  public ColumnPath mapValue() {
    return child(Element.MAP_VALUE);
  }

  public boolean isRoot() {
    return elements.isEmpty();
  }

  public Element last() {
    if (isRoot()) {
      throw new IllegalStateException("The root path has no last element");
    }
    return elements.getLast();
  }

  /** Renders this path for humans, in logs and error messages. */
  public String display() {
    return renderDottedPath();
  }

  /**
   * Serializes this path for the existing dotted-string connector contract.
   *
   * <p>This representation is lossy. Callers must detect when distinct structured paths produce the
   * same key rather than treating the string as unique identity; {@link LegacyDottedKeyIndex} does
   * this for them.
   */
  public String legacyDottedKey() {
    return renderDottedPath();
  }

  private String renderDottedPath() {
    StringBuilder result = new StringBuilder();
    for (Element element : elements) {
      switch (element.kind()) {
        case FIELD -> {
          if (!result.isEmpty()) {
            result.append('.');
          }
          result.append(element.name());
        }
        case ARRAY_ELEMENT -> result.append("[]");
        case MAP_KEY -> result.append(".key");
        case MAP_VALUE -> result.append("{}");
      }
    }
    return result.toString();
  }

  private ColumnPath child(Element element) {
    List<Element> result = new ArrayList<>(elements.size() + 1);
    result.addAll(elements);
    result.add(Objects.requireNonNull(element, "element"));
    return new ColumnPath(result);
  }

  @Override
  public String toString() {
    return display();
  }
}

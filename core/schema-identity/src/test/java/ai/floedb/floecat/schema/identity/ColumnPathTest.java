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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.schema.identity.ColumnPath.PathElement;
import org.junit.jupiter.api.Test;

class ColumnPathTest {

  @Test
  void aLiteralDottedFieldNameIsNotTheSamePathAsNesting() {
    ColumnPath literal = ColumnPath.ROOT.field("a.b");
    ColumnPath nested = ColumnPath.ROOT.field("a").field("b");

    // Both render identically, which is exactly why the rendering cannot be the identity key.
    assertThat(literal.display()).isEqualTo("a.b");
    assertThat(nested.display()).isEqualTo("a.b");
    assertThat(literal).isNotEqualTo(nested);
    assertThat(literal.hashCode()).isNotEqualTo(nested.hashCode());
    assertThat(literal.depth()).isEqualTo(1);
    assertThat(nested.depth()).isEqualTo(2);
  }

  @Test
  void namesContainingDisplaySyntaxStayDistinct() {
    ColumnPath bracketed = ColumnPath.ROOT.field("a[]");
    ColumnPath realArray = ColumnPath.ROOT.field("a").arrayElement();
    ColumnPath braced = ColumnPath.ROOT.field("m{}");
    ColumnPath realValue = ColumnPath.ROOT.field("m").mapValue();
    ColumnPath dottedKey = ColumnPath.ROOT.field("m.key");
    ColumnPath realKey = ColumnPath.ROOT.field("m").mapKey();

    assertThat(bracketed.display()).isEqualTo(realArray.display());
    assertThat(bracketed).isNotEqualTo(realArray);
    assertThat(braced.display()).isEqualTo(realValue.display());
    assertThat(braced).isNotEqualTo(realValue);
    assertThat(dottedKey.display()).isEqualTo(realKey.display());
    assertThat(dottedKey).isNotEqualTo(realKey);
  }

  @Test
  void rendersCollectionNotation() {
    assertThat(ColumnPath.ROOT.field("arr").arrayElement().field("x").display())
        .isEqualTo("arr[].x");
    assertThat(ColumnPath.ROOT.field("m").mapValue().arrayElement().display()).isEqualTo("m{}[]");
    assertThat(ColumnPath.ROOT.display()).isEmpty();
  }

  @Test
  void pathElementsEnforceTheirNameContract() {
    assertThatThrownBy(() -> PathElement.field(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires a name");
    assertThatThrownBy(() -> new PathElement(NodeKind.ARRAY_ELEMENT, "element"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot carry a name");
  }

  @Test
  void rootHasNoLastElement() {
    assertThat(ColumnPath.ROOT.isRoot()).isTrue();
    assertThatThrownBy(ColumnPath.ROOT::last).isInstanceOf(IllegalStateException.class);
  }
}

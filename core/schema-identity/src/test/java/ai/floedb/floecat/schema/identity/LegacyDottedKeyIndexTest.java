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
import static org.assertj.core.api.Assertions.entry;

import org.junit.jupiter.api.Test;

class LegacyDottedKeyIndexTest {

  @Test
  void distinctPathsKeepDistinctKeys() {
    LegacyDottedKeyIndex<String> index = LegacyDottedKeyIndex.create();

    index.add(ColumnPath.ROOT.field("a"), "first");
    index.add(ColumnPath.ROOT.field("b"), "second");

    assertThat(index.values()).containsExactly(entry("a", "first"), entry("b", "second"));
  }

  @Test
  void collidingPathsRetireTheSharedKeyForBoth() {
    ColumnPath dotted = ColumnPath.ROOT.field("a.b");
    ColumnPath nested = ColumnPath.ROOT.field("a").field("b");
    LegacyDottedKeyIndex<String> index = LegacyDottedKeyIndex.create();

    index.add(dotted, "first");
    index.add(nested, "second");

    assertThat(index.values()).isEmpty();
  }

  @Test
  void aRetiredKeyStaysRetiredForLaterPaths() {
    LegacyDottedKeyIndex<String> index = LegacyDottedKeyIndex.create();
    index.add(ColumnPath.ROOT.field("a.b"), "first");
    index.add(ColumnPath.ROOT.field("a").field("b"), "second");

    index.add(ColumnPath.ROOT.field("a.b"), "third");
    assertThat(index.values()).isEmpty();
  }

  @Test
  void reAddingAnEqualPathKeepsTheFirstValue() {
    LegacyDottedKeyIndex<String> index = LegacyDottedKeyIndex.create();

    index.add(ColumnPath.ROOT.field("a"), "first");
    index.add(ColumnPath.ROOT.field("a"), "second");

    assertThat(index.values()).containsExactly(entry("a", "first"));
  }

  @Test
  void containerPathsCollapsingOntoTheSameKeyAreRetired() {
    LegacyDottedKeyIndex<String> index = LegacyDottedKeyIndex.create();

    index.add(ColumnPath.ROOT.field("tags").arrayElement(), "element");
    index.add(ColumnPath.ROOT.field("tags[]"), "literal");

    assertThat(index.values()).isEmpty();
  }

  @Test
  void viewsDoNotAliasLaterMutations() {
    LegacyDottedKeyIndex<String> index = LegacyDottedKeyIndex.create();
    index.add(ColumnPath.ROOT.field("a"), "first");
    var snapshot = index.values();

    index.add(ColumnPath.ROOT.field("b"), "second");

    assertThat(snapshot).containsExactly(entry("a", "first"));
  }
}

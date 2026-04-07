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

package ai.floedb.floecat.client.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class CliArgsTest {

  // --- tail ---

  @Test
  void tailReturnsEmptyForSingleElement() {
    assertEquals(List.of(), CliArgs.tail(List.of("only")));
  }

  @Test
  void tailReturnsEmptyForEmptyList() {
    assertEquals(List.of(), CliArgs.tail(List.of()));
  }

  @Test
  void tailReturnsRestOfList() {
    assertEquals(List.of("b", "c"), CliArgs.tail(List.of("a", "b", "c")));
  }

  // --- tokenize ---

  @Test
  void tokenizeSimpleWords() {
    assertEquals(List.of("table", "create", "foo"), CliArgs.tokenize("table create foo"));
  }

  @Test
  void tokenizePreservesDoubleQuotedToken() {
    assertEquals(List.of("table", "\"my table\""), CliArgs.tokenize("table \"my table\""));
  }

  @Test
  void tokenizePreservesSingleQuotedToken() {
    assertEquals(List.of("table", "'my table'"), CliArgs.tokenize("table 'my table'"));
  }

  @Test
  void tokenizeHandlesBackslashEscape() {
    assertEquals(List.of("a\\b"), CliArgs.tokenize("a\\b"));
  }

  @Test
  void tokenizeThrowsOnUnclosedDoubleQuote() {
    assertThrows(IllegalArgumentException.class, () -> CliArgs.tokenize("table \"unclosed"));
  }

  @Test
  void tokenizeThrowsOnUnclosedSingleQuote() {
    assertThrows(IllegalArgumentException.class, () -> CliArgs.tokenize("table 'unclosed"));
  }

  @Test
  void tokenizeEmptyStringReturnsEmptyList() {
    assertEquals(List.of(), CliArgs.tokenize(""));
  }

  @Test
  void tokenizeCollapsesMidWhitespace() {
    assertEquals(List.of("a", "b"), CliArgs.tokenize("  a   b  "));
  }

  @Test
  void tokenizePreservesSpacesInsideDoubleQuotes() {
    // \fc catalog create "my catalog" → 3 tokens, quoted token includes the space
    assertEquals(
        List.of("catalog", "create", "\"my catalog\""),
        CliArgs.tokenize("catalog create \"my catalog\""));
  }

  @Test
  void tokenizePreservesSpacesInsideSingleQuotes() {
    assertEquals(
        List.of("catalog", "create", "'my catalog'"),
        CliArgs.tokenize("catalog create 'my catalog'"));
  }

  @Test
  void tokenizeMixedQuoteTypes() {
    assertEquals(List.of("a", "'foo'", "\"bar\""), CliArgs.tokenize("a 'foo' \"bar\""));
  }

  @Test
  void tokenizeFlagWithEqualsSign() {
    // --desc=hello is a single unquoted token; = is not a separator
    assertEquals(
        List.of("catalog", "create", "--desc=hello"),
        CliArgs.tokenize("catalog create --desc=hello"));
  }

  @Test
  void tokenizeTabAndMultipleSpacesCollapsed() {
    assertEquals(List.of("a", "b", "c"), CliArgs.tokenize("a  \t  b   c"));
  }

  @Test
  void tokenizeEscapeInsideDoubleQuotes() {
    // backslash escapes the following character even inside double quotes
    assertEquals(List.of("\"hello\\\"world\""), CliArgs.tokenize("\"hello\\\"world\""));
  }

  // --- parseFlag / parseStringFlag / parseIntFlag / parseLongFlag ---

  @Test
  void parseStringFlagReturnsValue() {
    assertEquals("bar", CliArgs.parseStringFlag(List.of("--foo", "bar"), "--foo", "default"));
  }

  @Test
  void parseStringFlagReturnsDefaultWhenAbsent() {
    assertEquals("default", CliArgs.parseStringFlag(List.of("--other", "x"), "--foo", "default"));
  }

  @Test
  void parseStringFlagReturnsDefaultWhenFlagIsLast() {
    assertEquals("default", CliArgs.parseStringFlag(List.of("--foo"), "--foo", "default"));
  }

  @Test
  void parseIntFlagReturnsValue() {
    assertEquals(42, CliArgs.parseIntFlag(List.of("--limit", "42"), "--limit", 0));
  }

  @Test
  void parseIntFlagReturnsDefaultWhenAbsent() {
    assertEquals(10, CliArgs.parseIntFlag(List.of(), "--limit", 10));
  }

  @Test
  void parseIntFlagReturnsDefaultOnBadValue() {
    assertEquals(5, CliArgs.parseIntFlag(List.of("--limit", "notanint"), "--limit", 5));
  }

  @Test
  void parseLongFlagReturnsValue() {
    assertEquals(99L, CliArgs.parseLongFlag(List.of("--snap", "99"), "--snap", 0L));
  }

  @Test
  void parseLongFlagReturnsDefaultWhenAbsent() {
    assertEquals(7L, CliArgs.parseLongFlag(List.of(), "--snap", 7L));
  }

  // --- hasFlag ---

  @Test
  void hasFlagReturnsTrueWhenPresent() {
    assertTrue(CliArgs.hasFlag(List.of("--json", "--verbose"), "--json"));
  }

  @Test
  void hasFlagReturnsFalseWhenAbsent() {
    assertFalse(CliArgs.hasFlag(List.of("--verbose"), "--json"));
  }

  @Test
  void hasFlagReturnsFalseOnEmptyList() {
    assertFalse(CliArgs.hasFlag(List.of(), "--json"));
  }
}

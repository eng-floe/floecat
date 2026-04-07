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

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.client.cli.util.FQNameParserUtil;
import ai.floedb.floecat.client.cli.util.NameRefUtil;
import ai.floedb.floecat.client.cli.util.Quotes;
import ai.floedb.floecat.common.rpc.NameRef;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ShellFqQuoteTest {

  private Shell shell;

  @BeforeEach
  void setUp() {
    shell = new Shell();
  }

  private List<String> splitPath(String s) {
    return FQNameParserUtil.segments(s);
  }

  private NameRef parseTable(String fq) {
    return NameRefUtil.nameRefForTable(fq);
  }

  private NameRef parseNsLeafAsName(String s) {
    return NamespaceCliSupport.nameRefForNamespace(s, false);
  }

  private NameRef parseNsLeafInPath(String s) {
    return NamespaceCliSupport.nameRefForNamespace(s, true);
  }

  private static String quoteIfNeeded(String s) {
    return Quotes.quoteIfNeeded(s);
  }

  private static String unquoteArg(String s) {
    return Quotes.unquote(s);
  }

  private String joinFqQuoted(String catalog, List<String> ns, String obj) {
    return NameRefUtil.joinFqQuoted(catalog, ns, obj);
  }

  private List<String> tokenize(String line) {
    return CliArgs.tokenize(line);
  }

  @Test
  void splitPathSupportsDoubleAndSingleQuotesAndEscapes() {
    assertEquals(List.of("a.b", "c d", "e"), splitPath("\"a.b\".'c d'.e"));
    assertEquals(List.of("a.b", "c"), splitPath("a\\.b.c"));
    assertEquals(List.of("a\\.b", "c"), splitPath("\"a\\\\.b\".c"));
    assertEquals(List.of("my ns", "leaf"), splitPath("\"my ns\" . 'leaf'"));
  }

  @Test
  void tokenizePreservesQuotedSegmentsAndEscapes() {
    var t = tokenize("namespace create \"cat one\".\"a.b\".'leaf c'");
    assertEquals(3, t.size());
    assertEquals("namespace", t.get(0));
    assertEquals("create", t.get(1));
    assertEquals("\"cat one\".\"a.b\".'leaf c'", t.get(2));
  }

  @Test
  void unquoteArgRoundTripForQuotesAndBackslashes() {
    String s1 = "\"a.b c\"";
    assertEquals("a.b c", unquoteArg(s1));
    assertEquals("\"a.b c\"", quoteIfNeeded(unquoteArg(s1)));

    String s2 = "'x\\'y'";
    assertEquals("x'y", unquoteArg(s2));

    String s3 = "\"x\\\"y\\\\z\"";
    assertEquals("x\"y\\z", unquoteArg(s3));
  }

  @Test
  void parseFqHandlesQuotedCatalogAndNamespaceAndObject() throws Exception {
    NameRef nr = parseTable("\"cat.one\".\"a.b\".\"leaf c\"");
    assertEquals("cat.one", nr.getCatalog());
    assertEquals(List.of("a.b"), nr.getPathList());
    assertEquals("leaf c", nr.getName());
  }

  @Test
  void joinFqQuotedQuotesOnlyWhenNecessary() {
    assertEquals("cat.ns.leaf", joinFqQuoted("cat", List.of("ns"), "leaf"));
    assertEquals("\"cat.one\".\"a.b\".leaf", joinFqQuoted("cat.one", List.of("a.b"), "leaf"));
    assertEquals(
        "\"cat one\".\"ns two\".\"leaf three\"",
        joinFqQuoted("cat one", List.of("ns two"), "leaf three"));
    assertEquals("cat.\"a.b\".\"c d\"", joinFqQuoted("cat", List.of("a.b", "c d"), null));
  }

  @Test
  void fqRoundTripCatalogNsObject() throws Exception {
    String fq = "\"cat.one\".\"a.b\".\"leaf c\"";
    NameRef nr = parseTable(fq);
    String rebuilt = joinFqQuoted(nr.getCatalog(), nr.getPathList(), nr.getName());
    assertEquals(fq, rebuilt);
  }

  @Test
  void nameRefForTablePrefixIncludesOptionalPrefixName() {
    NameRef nr = TableCliSupport.nameRefForTablePrefix("cat.ns1.ns2.orders_");

    assertEquals("cat", nr.getCatalog());
    assertEquals(List.of("ns1", "ns2"), nr.getPathList());
    assertEquals("orders_", nr.getName());
  }

  @Test
  void splitPathThrowsOnUnclosedQuoteSingle() {
    assertThrows(IllegalArgumentException.class, () -> splitPath("'x.y"));
  }

  @Test
  void splitPathThrowsOnUnclosedQuoteDouble() {
    assertThrows(IllegalArgumentException.class, () -> splitPath("\"a.b"));
  }

  @Test
  void tokenizeHandlesMixedQuotesAndEscapes() {
    var t = tokenize("table create \"cat one\".'ns two'.\"tab\\\" three\" --desc \"x\\\\y\"");
    assertEquals(5, t.size());
    assertEquals("table", t.get(0));
    assertEquals("create", t.get(1));
    assertEquals("\"cat one\".'ns two'.\"tab\\\" three\"", t.get(2));
    assertEquals("--desc", t.get(3));
    assertEquals("\"x\\\\y\"", t.get(4));
  }

  @Test
  void tokenizeThrowsOnUnclosedQuotes() {
    assertThrows(IllegalArgumentException.class, () -> tokenize("namespace get \"cat.ns"));
    assertThrows(IllegalArgumentException.class, () -> tokenize("namespace get 'cat.ns"));
  }

  @Test
  void splitPathConsecutiveDotsCollapseEmptySegments() {
    assertEquals(List.of("a", "b"), splitPath("a..b"));
    assertEquals(List.of("a", "b"), splitPath("a...b"));
  }

  @Test
  void splitPathTrailingBackslashBecomesLiteralBackslashSegment() {
    assertEquals(List.of("a", "\\"), splitPath("a.\\"));
  }

  @Test
  void splitPathPreservesInternalButTrimsLeadingTrailingSpaces() {
    assertEquals(List.of("a b", "c"), splitPath(" \"a b\" . c "));
    assertEquals(List.of("a b"), splitPath("\"  a b  \""));
  }

  @Test
  void splitPathEscapedQuoteCharsInsideQuotes() {
    assertEquals(List.of("a\"b", "c"), splitPath("\"a\\\"b\".c"));
    assertEquals(List.of("a'b", "c"), splitPath("'a\\'b'.c"));
  }

  @Test
  void splitPathEscapedBackslashInsideQuotes() {
    assertEquals(List.of("a\\b", "c"), splitPath("\"a\\\\b\".c"));
    assertEquals(List.of("a\\b", "c"), splitPath("'a\\\\b'.c"));
  }

  @Test
  void parseFqFlexibleRejectsMissingParts() {
    assertThrows(IllegalArgumentException.class, () -> parseTable("cat"));
    assertThrows(IllegalArgumentException.class, () -> parseTable("cat."));
    assertThrows(IllegalArgumentException.class, () -> parseNsLeafInPath(".ns"));
  }

  @Test
  void parseFqFlexibleAcceptsQuotedCatalogAndNestedNamespaces() throws Exception {
    var fq = "\"cat.one\".\"a.b\".\"c.d\"";
    NameRef nr = parseNsLeafAsName(fq);
    String rebuilt = joinFqQuoted(nr.getCatalog(), nr.getPathList(), nr.getName());
    assertEquals(fq, rebuilt);
    assertEquals("cat.one", nr.getCatalog());
    List<String> ns = (List<String>) nr.getPathList();
    assertEquals(List.of("a.b"), ns);
    assertEquals("c.d", nr.getName());
  }

  @Test
  void nameRefForNamespaceLeafInPathFlagBehaves() {
    NameRef leafAsName = parseNsLeafAsName("cat.\"a.b\".\"c d\"");
    assertEquals("cat", leafAsName.getCatalog());
    assertEquals(List.of("a.b"), leafAsName.getPathList());
    assertEquals("c d", leafAsName.getName());
    NameRef leafInPath = parseNsLeafInPath("cat.\"a.b\".\"c d\"");
    assertEquals("cat", leafInPath.getCatalog());
    assertEquals(List.of("a.b", "c d"), leafInPath.getPathList());
    assertEquals("", leafInPath.getName());
  }

  @Test
  void nameRefForTableRequiresObject() {
    assertThrows(IllegalArgumentException.class, () -> NameRefUtil.nameRefForTable("cat.ns"));
  }

  @Test
  void joinFqQuotedRoundTripsComplexNames() {
    String fq = joinFqQuoted("cat one", List.of("ns.two", " p q "), "leaf.r");
    assertEquals("\"cat one\".\"ns.two\".\" p q \".\"leaf.r\"", fq);
  }

  @Test
  void quoteIfNeededEscapesInnerDoubleQuotesAndBackslashes() {
    String quoted = quoteIfNeeded("He said \"hi\" \\ o/");
    assertEquals("\"He said \\\"hi\\\" \\\\ o/\"", quoted);
  }

  @Test
  void unquoteArgHandlesSingleAndDoubleQuotes() {
    assertEquals("a\"b", unquoteArg("\"a\\\"b\""));
    assertEquals("a'b", unquoteArg("'a\\'b'"));
    assertEquals("x\\y", unquoteArg("\"x\\\\y\""));
  }

  @Test
  void fqRoundTripDeepHierarchyAndEdgeCharacters() throws Exception {
    String fq = "\"cat.one\".\"a b\".\"c.d\".\"e\\\"f\"";
    NameRef nr = parseTable(fq);
    String rebuilt = joinFqQuoted(nr.getCatalog(), nr.getPathList(), nr.getName());
    assertEquals(fq, rebuilt);
  }

  @Test
  void parseFqFlexibleToleratesWhitespaceAroundDotsAndNames() throws Exception {
    NameRef nr = parseTable("  \"cat.one\"  .  \"a b\" .  \"c\"  ");
    assertEquals("cat.one", nr.getCatalog());
    assertEquals(List.of("a b"), nr.getPathList());
    assertEquals("c", nr.getName());
  }

  @Test
  void splitPathComplexRealisticCases() {
    assertEquals(List.of("a.b", "c", "d.e f", "g h"), splitPath("\"a.b\" . c . \"d.e f\" . 'g h'"));
    assertEquals(List.of("a.b", "c d", "e"), splitPath("a\\.b.\"c d\".e"));
  }

  @Test
  void nameRefForTablePrefixCapturesNameWhenPresentOrOmitsWhenMissing() {
    NameRef withPrefix = TableCliSupport.nameRefForTablePrefix("cat.ns.orders_");
    assertEquals("cat", withPrefix.getCatalog());
    assertEquals(List.of("ns"), withPrefix.getPathList());
    assertEquals("orders_", withPrefix.getName());

    NameRef withoutPrefix = TableCliSupport.nameRefForTablePrefix("cat.ns");
    assertEquals("cat", withoutPrefix.getCatalog());
    assertEquals(List.of("ns"), withoutPrefix.getPathList());
    assertEquals("", withoutPrefix.getName());
  }

  @Test
  void parseFqRespectsQuotedDotsEverywhere() {
    var nr = parseTable("\"cat.one\".\"a\\.b\".\"c.d\"");
    String catalog = nr.getCatalog();
    List<String> ns = (List<String>) nr.getPathList();
    String obj = nr.getName();

    assertEquals("cat.one", catalog);
    assertEquals(List.of("a\\.b"), ns);
    assertEquals("c.d", obj);
  }

  @Test
  void parseFqUsesFirstUnquotedDotForCatalogSplit() {
    var nr = parseTable("\"cat.one\".\"ns\".\"t\"");
    assertEquals("cat.one", nr.getCatalog());
    assertEquals(List.of("ns"), nr.getPathList());
    assertEquals("t", nr.getName());
  }

  @Test
  void parseFqMixedQuotesAndEscapedQuoteInside() {
    var nr = parseTable("\"cat one\".'ns two'.\"tab\\\" three\"");
    assertEquals("cat one", nr.getCatalog());
    List<String> ns = (List<String>) nr.getPathList();
    assertEquals(List.of("ns two"), ns);
    assertEquals("tab\" three", nr.getName());
  }

  @Test
  void fqRoundTripCanonicalQuotedDotsAndSpaces() {
    String input = "\"cat.one\".'a b'.\"c.d\"";
    var nr = parseTable(input);
    String catalog = nr.getCatalog();
    List<String> ns = (List<String>) nr.getPathList();
    String obj = nr.getName();

    String canonical = joinFqQuoted(catalog, ns, obj);
    assertEquals("\"cat.one\".\"a b\".\"c.d\"", canonical);
  }

  @Test
  void parseFqErrorsMissingNamespaceOrObject() {
    assertThrows(IllegalArgumentException.class, () -> parseTable("cat"));
    assertThrows(IllegalArgumentException.class, () -> parseTable("cat."));
    assertThrows(IllegalArgumentException.class, () -> parseTable("cat.ns"));
  }

  @Test
  void parseFqErrorsUnclosedQuotes() {
    assertThrows(IllegalArgumentException.class, () -> parseTable("\"cat.ns"));
    assertThrows(IllegalArgumentException.class, () -> parseTable("cat.\"ns"));
  }

  @Test
  void splitPathErrorsUnclosedQuotesSingleAndDouble() {
    assertThrows(IllegalArgumentException.class, () -> splitPath("'ns.part"));
    assertThrows(IllegalArgumentException.class, () -> splitPath("\"ns.part"));
  }

  @Test
  void splitPathHandlesEscapedDotOutsideQuotesAndInsideQuotes() {
    assertEquals(List.of("a.b", "c"), splitPath("a\\.b.c"));
    assertEquals(List.of("a\\.b", "c"), splitPath("\"a\\.b\".c"));
    assertEquals(List.of("a\\.b", "c"), splitPath("\"a\\\\.b\".c"));
  }

  @Test
  void splitPathSupportsSingleQuotesFully() {
    assertEquals(List.of("x y", "z"), splitPath("'x y'.z"));
    assertEquals(List.of("x.y", "z"), splitPath("'x.y'.z"));
  }

  @Test
  void tokenizeKeepsWholeFQAsOneTokenMixedQuotes() {
    var t = tokenize("table create \"cat one\".'ns two'.\"tab\\\" three\" --desc \"x\\\\y\"");
    assertEquals(5, t.size());
    assertEquals("table", t.get(0));
    assertEquals("create", t.get(1));
    assertEquals("\"cat one\".'ns two'.\"tab\\\" three\"", t.get(2));
    assertEquals("--desc", t.get(3));
    assertEquals("\"x\\\\y\"", t.get(4));
  }

  @Test
  void quoteUnquoteSymmetryBackslashesAndQuotes() {
    String raw = "He said \"hi\" \\ o/";
    String quoted = quoteIfNeeded(raw);
    assertEquals("\"He said \\\"hi\\\" \\\\ o/\"", quoted);
    assertEquals(raw, unquoteArg(quoted));
  }

  @Test
  void quoteIfNeededOnlyQuotesWhenNecessary() {
    assertEquals("plain", quoteIfNeeded("plain"));
    assertEquals("\"a b\"", quoteIfNeeded("a b"));
    assertEquals("\"a.b\"", quoteIfNeeded("a.b"));
    assertEquals("\"a\\\\b\"", quoteIfNeeded("a\\b"));
  }

  @Test
  void helpIncludesAnalyzeCommand() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream original = System.out;
    try {
      System.setOut(new PrintStream(baos));
      shell.printHelp();
    } finally {
      System.setOut(original);
    }
    String help = baos.toString();
    assertTrue(help.contains("analyze <tableFQ> [--columns c1,c2,...]"));
  }

  @Test
  void analyzeWithoutArgsPrintsUsage() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    StatsCliSupport.handle(
        "analyze", List.of(), new PrintStream(baos), null, null, null, null, null);
    assertTrue(baos.toString().contains("usage: analyze <tableFQ> [--columns c1,c2,...]"));
  }
}

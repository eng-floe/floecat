package ai.floedb.metacat.client.cli.util;

import ai.floedb.metacat.client.cli.csv.CsvListLexer;
import ai.floedb.metacat.client.cli.csv.CsvListParser;
import java.util.ArrayList;
import java.util.List;
import org.antlr.v4.runtime.*;

public final class CsvListParserUtil {

  public static List<String> items(String input) {
    if (input == null) {
      return List.of();
    }

    String s = input.trim();
    if (s.isEmpty()) {
      return List.of();
    }

    CharStream cs = CharStreams.fromString(s);
    CsvListLexer lexer = new CsvListLexer(cs);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    CsvListParser parser = new CsvListParser(tokens);

    CsvListParser.ListContext ctx = parser.list();

    List<String> out = new ArrayList<>(ctx.item().size());
    for (CsvListParser.ItemContext ic : ctx.item()) {
      Token t = ic.getStart();
      int type = t.getType();
      String text = ic.getText();

      if (type == CsvListLexer.DQSTR || type == CsvListLexer.SQSTR) {
        char quote = text.charAt(0);
        String body = text.substring(1, text.length() - 1);
        out.add(unescapeQuoted(body, quote));
      } else {
        out.add(unescapeRaw(text));
      }
    }
    return out;
  }

  private static String unescapeQuoted(String s, char quote) {
    StringBuilder b = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '\\' && i + 1 < s.length()) {
        char n = s.charAt(++i);
        if (n == quote || n == '\\' || n == ',') {
          b.append(n);
        } else {
          b.append('\\').append(n);
        }
      } else {
        b.append(c);
      }
    }
    return b.toString();
  }

  private static String unescapeRaw(String s) {
    StringBuilder b = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '\\' && i + 1 < s.length()) {
        char n = s.charAt(++i);
        if (n == ',' || n == '\\') {
          b.append(n);
        } else {
          b.append('\\').append(n);
        }
      } else {
        b.append(c);
      }
    }
    return b.toString().trim();
  }
}

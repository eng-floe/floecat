package ai.floedb.floecat.client.cli.util;

import ai.floedb.floecat.client.cli.fq.FQNameLexer;
import ai.floedb.floecat.client.cli.fq.FQNameParser;
import java.util.ArrayList;
import java.util.List;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;

public final class FQNameParserUtil {

  public static FQNameParser.FqnContext parse(String input) {
    CharStream cs = CharStreams.fromString(input);
    FQNameLexer lexer = new FQNameLexer(cs);
    lexer.removeErrorListeners();
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    FQNameParser parser = new FQNameParser(tokens);
    parser.removeErrorListeners();
    return parser.fqn();
  }

  public static List<String> segments(String input) {
    var ctx = parse(input);
    List<String> out = new ArrayList<>(ctx.segment().size());
    for (FQNameParser.SegmentContext seg : ctx.segment()) {
      Token t = seg.getStart();
      switch (t.getType()) {
        case FQNameParser.QUOTED_DOUBLE:
          out.add(unescapeQuoted(stripOuter(seg.getText()), '"', true));
          break;
        case FQNameParser.QUOTED_SINGLE:
          out.add(unescapeQuoted(stripOuter(seg.getText()), '\'', true));
          break;
        default:
          out.add(decodeUnquoted(seg.getText()));
      }
    }
    return out;
  }

  private static String stripOuter(String quoted) {
    return quoted.substring(1, quoted.length() - 1);
  }

  private static String unescapeQuoted(String body, char quote, boolean trim) {
    String s = trim ? body.trim() : body;
    StringBuilder b = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '\\' && i + 1 < s.length()) {
        char n = s.charAt(++i);
        if (n == quote) {
          b.append(quote);
        } else if (n == '\\') {
          b.append('\\');
        } else {
          b.append('\\').append(n);
        }
      } else {
        b.append(c);
      }
    }
    return b.toString();
  }

  private static String decodeUnquoted(String text) {
    StringBuilder b = new StringBuilder(text.length());
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == '\\' && i + 1 < text.length() && text.charAt(i + 1) == '.') {
        b.append('.');
        i++;
      } else {
        b.append(c);
      }
    }
    return b.toString();
  }
}

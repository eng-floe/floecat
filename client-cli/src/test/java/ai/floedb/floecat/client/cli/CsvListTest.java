package ai.floedb.floecat.client.cli;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

final class CsvListTest {

  private static Method csvListMethod;

  @BeforeAll
  static void setup() throws Exception {
    var cls = Class.forName("ai.floedb.floecat.client.cli.Shell");
    csvListMethod = cls.getDeclaredMethod("csvList", String.class);
    csvListMethod.setAccessible(true);
  }

  @SuppressWarnings("unchecked")
  private static List<String> csv(String s) {
    try {
      return (List<String>) csvListMethod.invoke(null, s);
    } catch (Exception e) {
      if (e.getCause() instanceof RuntimeException re) throw re;
      throw new RuntimeException(e);
    }
  }

  @Test
  void basicSplit() {
    assertEquals(List.of("a", "b", "c"), csv("a,b,c"));
  }

  @Test
  void quotedPreservesInternalCommas() {
    assertEquals(List.of("a,b", "c"), csv("\"a,b\",c"));
  }

  @Test
  void escapedCommaOutsideQuotes() {
    assertEquals(List.of("a,b", "c"), csv("a\\,b,c"));
  }

  @Test
  void unclosedQuoteThrows() {
    assertThrows(IllegalArgumentException.class, () -> csv("\"a,b"));
  }
}

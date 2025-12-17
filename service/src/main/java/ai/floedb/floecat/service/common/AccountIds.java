package ai.floedb.floecat.service.common;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class AccountIds {
  private AccountIds() {}

  public static String deterministicAccountId(String displayName) {
    if (displayName == null) {
      displayName = "";
    }
    return UUID.nameUUIDFromBytes((displayName).getBytes(StandardCharsets.UTF_8)).toString();
  }
}

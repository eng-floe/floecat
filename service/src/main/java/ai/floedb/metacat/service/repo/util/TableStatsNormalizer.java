package ai.floedb.metacat.service.repo.util;

import ai.floedb.metacat.catalog.rpc.TableStats;
import java.security.MessageDigest;
import java.util.TreeMap;

public final class TableStatsNormalizer {
  private TableStatsNormalizer() {}

  public static TableStats normalize(TableStats in) {
    var b = in.toBuilder();

    if (b.hasUpstream()) {
      var up = b.getUpstream().toBuilder();
      up.clearFetchedAt();
      up.clearCommitRef();
      b.setUpstream(up);
    }

    if (!b.getExtrasMap().isEmpty()) {
      var sorted = new TreeMap<>(b.getExtrasMap());
      b.clearExtras();
      b.putAllExtras(sorted);
    }

    return b.build();
  }

  public static String sha256Hex(byte[] bytes) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] dig = md.digest(bytes);
      StringBuilder sb = new StringBuilder(dig.length * 2);
      for (byte x : dig) {
        sb.append(String.format("%02x", x));
      }

      return sb.toString();
    } catch (Exception e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }
}

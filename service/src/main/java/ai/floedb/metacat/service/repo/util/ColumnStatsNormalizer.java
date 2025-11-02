package ai.floedb.metacat.service.repo.util;

import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.Ndv;
import ai.floedb.metacat.catalog.rpc.NdvApprox;
import ai.floedb.metacat.catalog.rpc.NdvSketch;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

public final class ColumnStatsNormalizer {
  private ColumnStatsNormalizer() {}

  public static ColumnStats normalize(ColumnStats in) {
    var b = in.toBuilder();

    if (b.hasUpstream()) {
      var up = b.getUpstream().toBuilder();
      up.clearFetchedAt();
      up.clearCommitRef();
      b.setUpstream(up);
    }

    if (b.hasNdv()) {
      Ndv.Builder ndv = b.getNdv().toBuilder();

      if (ndv.hasApprox()) {
        NdvApprox.Builder ab = ndv.getApprox().toBuilder();
        if (!ab.getParamsMap().isEmpty()) {
          var sorted = new TreeMap<>(ab.getParamsMap());
          ab.clearParams();
          ab.putAllParams(sorted);
        }
        ndv.setApprox(ab);
      }

      if (ndv.getSketchesCount() > 0) {
        List<NdvSketch> sketches = new ArrayList<>(ndv.getSketchesList().size());
        for (NdvSketch s : ndv.getSketchesList()) {
          NdvSketch.Builder sb = s.toBuilder();
          if (!sb.getParamsMap().isEmpty()) {
            var sorted = new TreeMap<>(sb.getParamsMap());
            sb.clearParams();
            sb.putAllParams(sorted);
          }
          sketches.add(sb.build());
        }

        Comparator<NdvSketch> sketchCmp =
            Comparator.comparing(
                    NdvSketch::getType, Comparator.nullsFirst(Comparator.naturalOrder()))
                .thenComparingInt(NdvSketch::getVersion)
                .thenComparing(
                    NdvSketch::getEncoding, Comparator.nullsFirst(Comparator.naturalOrder()))
                .thenComparing(
                    NdvSketch::getCompression, Comparator.nullsFirst(Comparator.naturalOrder()))
                .thenComparingInt(s -> s.getData().isEmpty() ? -1 : s.getData().size())
                .thenComparing(
                    s -> s.getData().isEmpty() ? null : sha256Hex(s.getData().toByteArray()),
                    Comparator.nullsFirst(Comparator.naturalOrder()));
        sketches.sort(sketchCmp);
        ndv.clearSketches();
        ndv.addAllSketches(sketches);
      }

      b.setNdv(ndv);
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
      var md = MessageDigest.getInstance("SHA-256");
      var dig = md.digest(bytes);
      var sb = new StringBuilder(dig.length * 2);
      for (byte x : dig) {
        sb.append(String.format("%02x", x));
      }

      return sb.toString();
    } catch (Exception e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  private static int nnCmp(String a, String b) {
    if (a == b) return 0;
    if (a == null) return -1;
    if (b == null) return 1;
    return a.compareTo(b);
  }
}

package ai.floedb.floecat.service.repo.util;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.NdvApprox;
import ai.floedb.floecat.catalog.rpc.NdvSketch;
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
        if (!ab.getPropertiesMap().isEmpty()) {
          var sorted = new TreeMap<>(ab.getPropertiesMap());
          ab.clearProperties();
          ab.putAllProperties(sorted);
        }
        ndv.setApprox(ab);
      }

      if (ndv.getSketchesCount() > 0) {
        List<NdvSketch> sketches = new ArrayList<>(ndv.getSketchesList().size());
        for (NdvSketch s : ndv.getSketchesList()) {
          NdvSketch.Builder sb = s.toBuilder();
          if (!sb.getPropertiesMap().isEmpty()) {
            var sorted = new TreeMap<>(sb.getPropertiesMap());
            sb.clearProperties();
            sb.putAllProperties(sorted);
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

    if (!b.getPropertiesMap().isEmpty()) {
      var sorted = new TreeMap<>(b.getPropertiesMap());
      b.clearProperties();
      b.putAllProperties(sorted);
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
}

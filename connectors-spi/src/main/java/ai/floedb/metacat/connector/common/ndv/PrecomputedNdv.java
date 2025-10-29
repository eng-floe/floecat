package ai.floedb.metacat.connector.common.ndv;

import java.util.Map;

public interface PrecomputedNdv extends NdvProvider {
  Map<String, Long> estimatesByName();
}

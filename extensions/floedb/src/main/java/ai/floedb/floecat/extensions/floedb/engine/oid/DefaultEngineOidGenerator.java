package ai.floedb.floecat.extensions.floedb.engine.oid;

import static java.nio.charset.StandardCharsets.UTF_8;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

/**
 * Default deterministic generator that hashes the resource hint key into a positive OID.
 *
 * <p>Uses rejection sampling to enforce {@link #MIN_OID} without bias. Deterministic for a given
 * (resourceId, payloadType, baseSalt) because retry keys are derived deterministically.
 *
 * <p>Collisions are still possible (31-bit space). Callers that require uniqueness must detect
 * duplicates and retry with a different salt (and persist the chosen salt).
 */
public final class DefaultEngineOidGenerator implements EngineOidGenerator {

  static final int MIN_OID = 16_384;
  private static final int MAX_RETRIES = 1_024;

  @Override
  public int generate(ResourceId id, String payloadType, String baseSalt) {
    Objects.requireNonNull(id, "id");

    String normalizedPayload = normalizePayloadType(payloadType);
    String normalizedSalt = normalizeSalt(baseSalt);

    for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
      String key =
          normalizedPayload
              + "|"
              + canonicalResourceId(id)
              + "|"
              + normalizedSalt
              + "|attempt="
              + attempt;

      UUID uuid = UUID.nameUUIDFromBytes(key.getBytes(UTF_8));
      long bits = uuid.getMostSignificantBits() ^ uuid.getLeastSignificantBits();
      int candidate = (int) (bits & 0x7FFFFFFF); // 0..Integer.MAX_VALUE

      if (candidate >= MIN_OID) {
        return candidate;
      }
    }

    throw new IllegalStateException(
        "Unable to generate OID >= " + MIN_OID + " after " + MAX_RETRIES + " attempts");
  }

  private static String normalizePayloadType(String payloadType) {
    if (payloadType == null) {
      return "";
    }
    String trimmed = payloadType.trim().toLowerCase(Locale.ROOT);
    return trimmed.isEmpty() ? "" : trimmed;
  }

  private static String canonicalResourceId(ResourceId id) {
    return id.getAccountId() + ":" + id.getKind() + ":" + id.getId();
  }

  private static String normalizeSalt(String salt) {
    if (salt == null) return "";
    return salt.trim(); // keep case-sensitive
  }
}

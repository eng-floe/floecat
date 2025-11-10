package ai.floedb.metacat.service.common;

import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.Precondition;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.connector.rpc.NamespacePath;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.util.BaseResourceRepository;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import ai.floedb.metacat.storage.errors.StorageAbortRetryableException;
import ai.floedb.metacat.storage.errors.StorageConflictException;
import ai.floedb.metacat.storage.errors.StorageCorruptionException;
import ai.floedb.metacat.storage.errors.StorageNotFoundException;
import ai.floedb.metacat.storage.errors.StoragePreconditionFailedException;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.inject.Inject;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;

public abstract class BaseServiceImpl {
  @Inject PrincipalProvider principal;

  protected final Clock clock = Clock.systemUTC();

  protected static final Duration BACKOFF_MIN = Duration.ofMillis(5);
  protected static final Duration BACKOFF_MAX = Duration.ofMillis(200);
  protected static final double JITTER = 0.5;
  protected static final int RETRIES = 8;

  @ConfigProperty(name = "metacat.idempotency.ttl-seconds", defaultValue = "900")
  protected long idempotencyTtlSeconds;

  protected long idempotencyTtlSeconds() {
    return idempotencyTtlSeconds;
  }

  protected <T> Uni<T> run(Supplier<T> body) {
    Uni<T> u = Uni.createFrom().item(body);
    return u.runSubscriptionOn(Infrastructure.getDefaultExecutor());
  }

  protected <T> Uni<T> runWithRetry(Supplier<T> body) {
    return run(body)
        .onFailure(
            t ->
                t instanceof BaseResourceRepository.AbortRetryableException
                    || t instanceof StorageAbortRetryableException)
        .retry()
        .withBackOff(BACKOFF_MIN, BACKOFF_MAX)
        .withJitter(JITTER)
        .atMost(RETRIES);
  }

  protected <T> Uni<T> mapFailures(Uni<T> u, String corrId) {
    return u.onFailure().transform(t -> toStatus(t, corrId));
  }

  private StatusRuntimeException toStatus(Throwable t, String corrId) {
    if (t instanceof StatusRuntimeException sre) {
      return sre;
    }

    if (t instanceof BaseResourceRepository.NameConflictException
        || t instanceof StorageConflictException) {
      return GrpcErrors.conflict(corrId, null, null, t);
    }
    if (t instanceof BaseResourceRepository.PreconditionFailedException
        || t instanceof StoragePreconditionFailedException) {
      return GrpcErrors.preconditionFailed(corrId, null, null, t);
    }
    if (t instanceof BaseResourceRepository.NotFoundException
        || t instanceof StorageNotFoundException) {
      return GrpcErrors.notFound(corrId, null, null, t);
    }
    if (t instanceof BaseResourceRepository.AbortRetryableException
        || t instanceof StorageAbortRetryableException) {
      return GrpcErrors.aborted(corrId, null, null, t);
    }
    if (t instanceof BaseResourceRepository.CorruptionException
        || t instanceof StorageCorruptionException) {
      return GrpcErrors.internal(corrId, null, null, t);
    }
    if (t instanceof IllegalArgumentException) {
      return GrpcErrors.invalidArgument(corrId, null, null, t);
    }
    if (t instanceof TimeoutException) {
      return GrpcErrors.timeout(corrId, null, null, t);
    }
    if (t instanceof CancellationException) {
      return GrpcErrors.cancelled(corrId, null, null, t);
    }

    return GrpcErrors.internal(corrId, null, null, t);
  }

  protected void ensureKind(
      ResourceId resourceId, ResourceKind expected, String field, String correlationId) {
    if (resourceId == null || resourceId.getKind() != expected) {
      throw GrpcErrors.invalidArgument(correlationId, "kind", Map.of("field", field));
    }
  }

  protected String mustNonEmpty(String inputString, String name, String corrId) {
    if (inputString == null || inputString.isBlank()) {
      throw GrpcErrors.invalidArgument(corrId, "field", Map.of("field", name));
    }
    return inputString;
  }

  protected void enforcePreconditions(
      String correlationId, MutationMeta metadata, Precondition precondition) {
    if (precondition == null) {
      return;
    }

    boolean checkVer = precondition.getExpectedVersion() > 0;

    boolean checkTag =
        precondition.getExpectedEtag() != null && !precondition.getExpectedEtag().isBlank();

    if (checkVer && metadata.getPointerVersion() != precondition.getExpectedVersion()) {
      throw GrpcErrors.preconditionFailed(
          correlationId,
          "version_mismatch",
          Map.of(
              "expected",
              Long.toString(precondition.getExpectedVersion()),
              "actual",
              Long.toString(metadata.getPointerVersion())));
    }
    if (checkTag && !metadata.getEtag().equals(precondition.getExpectedEtag())) {
      throw GrpcErrors.preconditionFailed(
          correlationId,
          "etag_mismatch",
          Map.of("expected", precondition.getExpectedEtag(), "actual", metadata.getEtag()));
    }
  }

  protected String correlationId() {
    var pctx = principal != null ? principal.get() : null;
    return pctx != null ? pctx.getCorrelationId() : "";
  }

  protected String deterministicUuid(String tenant, String kind, String key) {
    var name = (tenant + ":" + kind + ":" + key).getBytes(StandardCharsets.UTF_8);
    return UUID.nameUUIDFromBytes(name).toString();
  }

  protected static String prettyNamespacePath(List<String> parents, String leaf) {
    var parts = new ArrayList<>(parents);
    parts.add(leaf);
    return String.join(".", parts);
  }

  protected Timestamp nowTs() {
    return Timestamps.fromMillis(clock.millis());
  }

  public static final class Enforcers {
    public static void enforce(MutationMeta meta, Precondition p, String corr) {
      if (p == null) {
        return;
      }

      final boolean checkVer = p.getExpectedVersion() > 0L;
      final boolean checkTag = p.getExpectedEtag() != null && !p.getExpectedEtag().isBlank();

      if (checkVer && meta.getPointerVersion() != p.getExpectedVersion()) {
        throw GrpcErrors.preconditionFailed(
            corr,
            "version_mismatch",
            Map.of(
                "expected", Long.toString(p.getExpectedVersion()),
                "actual", Long.toString(meta.getPointerVersion())));
      }
      if (checkTag && !Objects.equals(p.getExpectedEtag(), meta.getEtag())) {
        throw GrpcErrors.preconditionFailed(
            corr,
            "etag_mismatch",
            Map.of("expected", p.getExpectedEtag(), "actual", meta.getEtag()));
      }
    }
  }

  protected static String hashFingerprint(byte[] data) {
    try {
      var md = java.security.MessageDigest.getInstance("SHA-256");
      return java.util.Base64.getEncoder().encodeToString(md.digest(data));
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not supported", e);
    }
  }

  protected static String trimToNull(String s) {
    if (s == null) {
      return null;
    }

    String t = s.trim();
    return t.isEmpty() ? null : t;
  }

  protected static String trimOrEmpty(String s) {
    return (s == null) ? "" : s.trim();
  }

  protected static Map<String, String> cleanKeysOnly(Map<String, String> in) {
    if (in == null || in.isEmpty()) {
      return java.util.Map.of();
    }

    var out = new LinkedHashMap<String, String>();
    for (var e : in.entrySet()) {
      String k = e.getKey() == null ? "" : e.getKey().trim();
      if (k.isEmpty()) {
        throw new IllegalArgumentException("options contain a blank key");
      }
      out.put(k, e.getValue());
    }
    return Collections.unmodifiableMap(out);
  }

  protected static List<List<String>> toPathsClean(List<NamespacePath> in) {
    if (in == null || in.isEmpty()) return java.util.List.of();
    return in.stream()
        .map(
            np ->
                np.getSegmentsList().stream()
                    .map(seg -> seg == null ? "" : seg.trim())
                    .filter(seg -> !seg.isEmpty())
                    .toList())
        .filter(path -> !path.isEmpty())
        .toList();
  }

  protected static List<String> normalizeSelectors(List<String> cols) {
    if (cols == null || cols.isEmpty()) return List.of();
    var seen = new LinkedHashSet<String>();
    for (String c : cols) {
      if (c == null) {
        continue;
      }

      String t = c.trim();
      if (!t.isEmpty()) {
        seen.add(t);
      }
    }
    for (String sel : seen) {
      if (sel.startsWith("#")) {
        String digits = sel.substring(1);
        if (digits.isEmpty() || !digits.chars().allMatch(Character::isDigit)) {
          throw new IllegalArgumentException("Invalid column id selector: " + sel);
        }
      }
    }
    return java.util.List.copyOf(seen);
  }

  protected static String cleanUri(String uri) {
    if (uri == null) {
      throw GrpcErrors.invalidArgument("?", null, Map.of("field", "uri"));
    }

    String t = uri.trim();
    try {
      var u = URI.create(t);
      if (u.getScheme() == null || u.getScheme().isBlank()) {
        throw new IllegalArgumentException("uri must include a scheme");
      }
    } catch (IllegalArgumentException e) {
      throw e;
    }
    return t;
  }

  protected static boolean maskTargets(FieldMask mask, String path) {
    return mask != null && mask.getPathsList().contains(path);
  }

  protected static boolean maskTargetsUnder(FieldMask mask, String container) {
    if (mask == null) return false;
    String prefix = container + ".";
    for (var p : mask.getPathsList()) {
      if (p.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  protected static Set<String> normalizedMaskPaths(FieldMask mask) {
    var out = new LinkedHashSet<String>();
    if (mask == null) {
      return out;
    }

    for (String p : mask.getPathsList()) {
      if (p == null) {
        continue;
      }
      var t = p.trim().toLowerCase();
      if (!t.isEmpty()) {
        out.add(t);
      }
    }
    return out;
  }

  protected static String nullSafeId(ai.floedb.metacat.common.rpc.ResourceId rid) {
    return (rid == null) ? "" : rid.getId();
  }

  protected static String normalizeName(String in) {
    if (in == null) {
      return "";
    }

    String t = Normalizer.normalize(in.trim(), Normalizer.Form.NFKC);
    t = t.replaceAll("\\s+", " ");
    return t;
  }
}

package ai.floedb.metacat.service.common;

import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.Precondition;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.util.BaseResourceRepository;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import ai.floedb.metacat.storage.errors.StorageAbortRetryableException;
import ai.floedb.metacat.storage.errors.StorageConflictException;
import ai.floedb.metacat.storage.errors.StorageCorruptionException;
import ai.floedb.metacat.storage.errors.StorageNotFoundException;
import ai.floedb.metacat.storage.errors.StoragePreconditionFailedException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public abstract class BaseServiceImpl {
  @Inject PrincipalProvider principal;

  protected final Clock clock = Clock.systemUTC();

  protected static final Duration BACKOFF_MIN = Duration.ofMillis(5);
  protected static final Duration BACKOFF_MAX = Duration.ofMillis(200);
  protected static final double JITTER = 0.5;
  protected static final int RETRIES = 8;

  protected static final long IDEMPOTENCY_TTL_SECONDS = 86_400L;

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
      throw GrpcErrors.invalidArgument(correlationId, "field", Map.of("field", field));
    }
  }

  protected String mustNonEmpty(String inputString, String name, String corrId) {
    if (inputString == null || inputString.isBlank()) {
      throw GrpcErrors.invalidArgument(corrId, "kind", Map.of("field", name));
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

  protected int parseIntToken(String token, String corrId) {
    if (token == null || token.isEmpty()) {
      return 0;
    }

    try {
      return Integer.parseInt(token);
    } catch (NumberFormatException nfe) {
      throw GrpcErrors.invalidArgument(corrId, "page_token.invalid", Map.of("page_token", token));
    }
  }

  protected String correlationId() {
    var pctx = principal != null ? principal.get() : null;
    return pctx != null ? pctx.getCorrelationId() : "";
  }

  protected String deterministicUuid(String tenant, String kind, String key) {
    var name = (kind + ":" + key).getBytes(StandardCharsets.UTF_8);
    return UUID.nameUUIDFromBytes(name).toString();
  }

  protected static String prettyNamespacePath(List<String> parents, String leaf) {
    var parts = new ArrayList<>(parents);
    parts.add(leaf);
    return String.join("/", parts);
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
}

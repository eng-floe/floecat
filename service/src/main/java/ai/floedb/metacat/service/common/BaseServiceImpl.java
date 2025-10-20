package ai.floedb.metacat.service.common;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.catalog.rpc.Precondition;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;

public abstract class BaseServiceImpl {
  @Inject PrincipalProvider principal;

  private final Clock clock = Clock.systemUTC();

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
    .onFailure(BaseRepository.AbortRetryableException.class)
    .retry()
      .withBackOff(BACKOFF_MIN, BACKOFF_MAX)
      .withJitter(JITTER)
      .atMost(RETRIES);
}

  protected <T> Uni<T> mapFailures(Uni<T> u, String corrId) {
    return u.onFailure().transform(t -> toStatus(t, corrId));
  }

  private StatusRuntimeException toStatus(Throwable t, String corrId) {
    if (t instanceof StatusRuntimeException sre) return sre;

    if (t instanceof BaseRepository.NameConflictException) {
      return GrpcErrors.conflict(corrId, "conflict.name_already_exists",
          Map.of("cause", t.getClass().getSimpleName()));
    }
    if (t instanceof BaseRepository.PreconditionFailedException) {
      return GrpcErrors.preconditionFailed(corrId, "precondition.failed",
          Map.of("cause", "optimistic_concurrency"));
    }
    if (t instanceof BaseRepository.NotFoundException) {
      return GrpcErrors.notFound(corrId, "not_found", Map.of());
    }
    if (t instanceof BaseRepository.AbortRetryableException) {
      return GrpcErrors.aborted(corrId, "aborted.retryable", Map.of());
    }
    if (t instanceof BaseRepository.CorruptionException) {
      return GrpcErrors.internal(corrId, "internal.corruption", Map.of());
    }

    if (t instanceof IllegalArgumentException) {
      return GrpcErrors.invalidArgument(corrId, "invalid_argument", Map.of());
    }
    if (t instanceof TimeoutException) {
      return GrpcErrors.timeout(corrId, "timeout", Map.of());
    }
    if (t instanceof CancellationException) {
      return GrpcErrors.cancelled(corrId, "cancelled", Map.of());
    }

    return GrpcErrors.internal(
        corrId,
        "internal.unexpected",
        Map.of("cause", t.getClass().getName()));
  }

  protected void ensureKind(ResourceId rid, ResourceKind want, String field, String corrId) {
    if (rid == null || rid.getKind() != want) {
      throw GrpcErrors.invalidArgument(corrId, null, Map.of("field", field));
    }
  }

  protected String mustNonEmpty(String v, String name, String corrId) {
    if (v == null || v.isBlank()) {
      throw GrpcErrors.invalidArgument(corrId, null, Map.of("field", name));
    }
    return v;
  }

  protected void enforcePreconditions(String corrId, MutationMeta cur, Precondition pc) {
    if (pc == null) return;
    boolean checkVer = pc.getExpectedVersion() > 0;
    boolean checkTag = pc.getExpectedEtag() != null && !pc.getExpectedEtag().isBlank();
    if (checkVer && cur.getPointerVersion() != pc.getExpectedVersion()) {
      throw GrpcErrors.preconditionFailed(
          corrId, "version_mismatch",
          Map.of("expected", Long.toString(pc.getExpectedVersion()),
                 "actual",   Long.toString(cur.getPointerVersion())));
    }
    if (checkTag && !cur.getEtag().equals(pc.getExpectedEtag())) {
      throw GrpcErrors.preconditionFailed(
          corrId, "etag_mismatch",
          Map.of("expected", pc.getExpectedEtag(), "actual", cur.getEtag()));
    }
  }

  protected int parseIntToken(String token, String corrId) {
    if (token == null || token.isEmpty()) return 0;
    try {
      return Integer.parseInt(token);
    } catch (NumberFormatException nfe) {
      throw GrpcErrors.invalidArgument(corrId, "page_token.invalid", Map.of("page_token", token));
    }
  }

  protected String corrId() {
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
    return String.join("/", parts);
  }

  protected Timestamp nowTs() {
    return Timestamps.fromMillis(clock.millis());
  }
}

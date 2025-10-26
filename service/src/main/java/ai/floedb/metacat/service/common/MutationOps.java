package ai.floedb.metacat.service.common;

import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.Precondition;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.storage.IdempotencyStore;
import ai.floedb.metacat.service.storage.util.IdempotencyGuard;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public final class MutationOps {

  public static final class OpResult<T> {
    public final T body;
    public final MutationMeta meta;

    public OpResult(T body, MutationMeta meta) {
      this.body = body;
      this.meta = meta;
    }
  }

  @FunctionalInterface
  public interface Creator<T> {
    IdempotencyGuard.CreateResult<T> create();
  }

  @FunctionalInterface
  public interface Fingerprinter {
    byte[] fingerprint();
  }

  @FunctionalInterface
  public interface ThrowingParser<T> {
    T parse(byte[] bytes) throws Exception;
  }

  public static <T> OpResult<T> create(
      String tenant,
      String operationName,
      String idempotencyKey,
      Fingerprinter fingerprint,
      Creator<T> creator,
      Function<T, MutationMeta> metaOf,
      Function<T, byte[]> serializer,
      Function<byte[], T> parser,
      IdempotencyStore idemStore,
      Timestamp now,
      long ttlSeconds,
      Supplier<String> corrIdSupplier) {

    T body =
        IdempotencyGuard.runOnce(
            tenant,
            operationName,
            idempotencyKey,
            fingerprint.fingerprint(),
            creator::create,
            metaOf,
            serializer,
            parser,
            idemStore,
            ttlSeconds,
            now,
            corrIdSupplier);

    return new OpResult<>(body, metaOf.apply(body));
  }

  public static <T extends Message> OpResult<T> createProto(
      String tenant,
      String operationName,
      String idempotencyKey,
      Fingerprinter fingerprint,
      Creator<T> creator,
      Function<T, MutationMeta> metaOf,
      IdempotencyStore idempotencyStore,
      Timestamp now,
      long ttlSeconds,
      Supplier<String> correlationIdSupplier,
      ThrowingParser<T> parser) {

    return create(
        tenant,
        operationName,
        idempotencyKey,
        fingerprint,
        creator,
        metaOf,
        Message::toByteArray,
        bytes -> {
          try {
            return parser.parse(bytes);
          } catch (Exception e) {
            throw new BaseRepository.CorruptionException("idempotency_parse_failed", e);
          }
        },
        idempotencyStore,
        now,
        ttlSeconds,
        correlationIdSupplier);
  }

  public static final class PageIn {
    public final int limit;
    public final String token;

    public PageIn(PageRequest pr) {
      this.limit = (pr != null && pr.getPageSize() > 0) ? pr.getPageSize() : 50;
      this.token = (pr != null) ? pr.getPageToken() : "";
    }
  }

  public static PageIn pageIn(PageRequest pr) {
    return new PageIn(pr);
  }

  public static PageResponse pageOut(String next, int total) {
    return PageResponse.newBuilder().setNextPageToken(next).setTotalSize(total).build();
  }

  @FunctionalInterface
  public interface UpdateAttempt {
    boolean run(long expectedVersion)
        throws BaseRepository.NameConflictException, BaseRepository.PreconditionFailedException;
  }

  public static void updateWithPreconditions(
      Supplier<MutationMeta> metaSupplier,
      Precondition precondition,
      UpdateAttempt attempt,
      Supplier<MutationMeta> nowMetaSupplier,
      String correlationId,
      String entity,
      Map<String, String> conflictKVs) {

    final MutationMeta meta = requireMeta(metaSupplier, correlationId, entity);

    BaseServiceChecks.enforcePreconditions(correlationId, meta, precondition);

    final long expected = meta.getPointerVersion();
    try {
      boolean ok = attempt.run(expected);
      if (!ok) {
        final var nowMeta = nowMetaSupplier.get();
        throw GrpcErrors.preconditionFailed(
            correlationId,
            "version_mismatch",
            Map.of(
                "expected", Long.toString(expected),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      }
    } catch (BaseRepository.NameConflictException nce) {
      throw GrpcErrors.conflict(correlationId, entity + ".already_exists", conflictKVs);
    } catch (BaseRepository.PreconditionFailedException pfe) {
      final var nowMeta = nowMetaSupplier.get();
      throw GrpcErrors.preconditionFailed(
          correlationId,
          "version_mismatch",
          Map.of(
              "expected", Long.toString(expected),
              "actual", Long.toString(nowMeta.getPointerVersion())));
    }
  }

  @FunctionalInterface
  public interface DeleteAttempt {
    boolean run(long expectedVersion)
        throws BaseRepository.PreconditionFailedException, BaseRepository.NotFoundException;
  }

  public static MutationMeta deleteWithPreconditions(
      Supplier<MutationMeta> metaSupplier,
      Precondition precondition,
      DeleteAttempt attempt,
      Supplier<MutationMeta> safeMetaSupplier,
      String correlationId,
      String entity,
      Map<String, String> notFoundKVs) {

    final MutationMeta meta;
    try {
      meta = requireMeta(metaSupplier, correlationId, entity);
    } catch (BaseRepository.NotFoundException e) {
      throw GrpcErrors.notFound(correlationId, entity, notFoundKVs);
    }

    BaseServiceChecks.enforcePreconditions(correlationId, meta, precondition);
    final long expected = meta.getPointerVersion();

    try {
      boolean ok = attempt.run(expected);
      if (!ok) {
        final var nowMeta = safeMetaSupplier.get();
        throw GrpcErrors.preconditionFailed(
            correlationId,
            "version_mismatch",
            Map.of(
                "expected", Long.toString(expected),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      }
    } catch (BaseRepository.NotFoundException nfe) {
      throw GrpcErrors.notFound(correlationId, entity, notFoundKVs);
    } catch (BaseRepository.PreconditionFailedException pfe) {
      final var nowMeta = safeMetaSupplier.get();
      throw GrpcErrors.preconditionFailed(
          correlationId,
          "version_mismatch",
          Map.of(
              "expected", Long.toString(expected),
              "actual", Long.toString(nowMeta.getPointerVersion())));
    }

    return meta;
  }

  private static MutationMeta requireMeta(
      Supplier<MutationMeta> metaSupplier, String correlationId, String entity) {
    final var m = metaSupplier.get();
    if (m == null) {
      throw new BaseRepository.NotFoundException("meta null");
    }
    return m;
  }

  public static final class BaseServiceChecks {

    public static void enforcePreconditions(
        String correlationId, MutationMeta meta, Precondition precondition) {
      if (precondition == null) {
        return;
      }

      boolean emptyVersion = precondition.getExpectedVersion() == 0L;
      boolean emptyEtag =
          precondition.getExpectedEtag() == null || precondition.getExpectedEtag().isBlank();
      if (emptyVersion && emptyEtag) {
        return;
      }

      BaseServiceImpl.Enforcers.enforce(meta, precondition, correlationId);
    }
  }
}

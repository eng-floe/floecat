/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.common;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.service.error.impl.GeneratedErrorMessages;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
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
      String account,
      String operationName,
      String idempotencyKey,
      Fingerprinter fingerprint,
      Creator<T> creator,
      Function<T, MutationMeta> metaOf,
      Function<T, byte[]> serializer,
      Function<byte[], T> parser,
      IdempotencyRepository idemStore,
      Timestamp now,
      long ttlSeconds,
      Supplier<String> corrIdSupplier) {

    var result =
        IdempotencyGuard.runOnce(
            account,
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

    return new OpResult<>(result.resource(), result.meta());
  }

  public static <T extends Message> OpResult<T> createProto(
      String account,
      String operationName,
      String idempotencyKey,
      Fingerprinter fingerprint,
      Creator<T> creator,
      Function<T, MutationMeta> metaOf,
      IdempotencyRepository idempotencyStore,
      Timestamp now,
      long ttlSeconds,
      Supplier<String> correlationIdSupplier,
      ThrowingParser<T> parser) {

    return create(
        account,
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
            throw new BaseResourceRepository.CorruptionException("idempotency_parse_failed", e);
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
        throws BaseResourceRepository.NameConflictException,
            BaseResourceRepository.PreconditionFailedException;
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
            GeneratedErrorMessages.MessageKey.VERSION_MISMATCH,
            Map.of(
                "expected", Long.toString(expected),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      }
    } catch (BaseResourceRepository.NameConflictException nce) {
      throw GrpcErrors.conflict(
          correlationId, GeneratedErrorMessages.bySuffix(entity + ".already_exists"), conflictKVs);
    } catch (BaseResourceRepository.PreconditionFailedException pfe) {
      final var nowMeta = nowMetaSupplier.get();
      throw GrpcErrors.preconditionFailed(
          correlationId,
          GeneratedErrorMessages.MessageKey.VERSION_MISMATCH,
          Map.of(
              "expected", Long.toString(expected),
              "actual", Long.toString(nowMeta.getPointerVersion())));
    }
  }

  @FunctionalInterface
  public interface DeleteAttempt {
    boolean run(long expectedVersion)
        throws BaseResourceRepository.PreconditionFailedException,
            BaseResourceRepository.NotFoundException;
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
    } catch (BaseResourceRepository.NotFoundException e) {
      throw GrpcErrors.notFound(
          correlationId, GeneratedErrorMessages.bySuffix(entity), notFoundKVs);
    }

    BaseServiceChecks.enforcePreconditions(correlationId, meta, precondition);
    boolean callerCares = BaseServiceImpl.hasMeaningfulPrecondition(precondition);
    final long expected = meta.getPointerVersion();

    try {
      boolean ok = attempt.run(expected);
      if (!ok) {
        final var nowMeta = safeMetaSupplier.get();
        if (!callerCares) {
          return nowMeta;
        }
        if (nowMeta.getPointerVersion() == 0L) {
          throw GrpcErrors.notFound(
              correlationId, GeneratedErrorMessages.bySuffix(entity), notFoundKVs);
        }
        throw GrpcErrors.preconditionFailed(
            correlationId,
            GeneratedErrorMessages.MessageKey.VERSION_MISMATCH,
            Map.of(
                "expected", Long.toString(expected),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      }
    } catch (BaseResourceRepository.NotFoundException nfe) {
      throw GrpcErrors.notFound(
          correlationId, GeneratedErrorMessages.bySuffix(entity), notFoundKVs);
    } catch (BaseResourceRepository.PreconditionFailedException pfe) {
      final var nowMeta = safeMetaSupplier.get();
      if (!callerCares) {
        return nowMeta;
      }
      if (nowMeta.getPointerVersion() == 0L) {
        throw GrpcErrors.notFound(
            correlationId, GeneratedErrorMessages.bySuffix(entity), notFoundKVs);
      }
      throw GrpcErrors.preconditionFailed(
          correlationId,
          GeneratedErrorMessages.MessageKey.VERSION_MISMATCH,
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
      throw new BaseResourceRepository.NotFoundException("meta null");
    }
    return m;
  }

  public static final class BaseServiceChecks {

    public static void enforcePreconditions(
        String correlationId, MutationMeta meta, Precondition precondition) {
      if (precondition == null) {
        return;
      }

      boolean emptyVersion = !precondition.hasExpectedVersion();
      boolean emptyEtag =
          precondition.getExpectedEtag() == null || precondition.getExpectedEtag().isBlank();
      if (emptyVersion && emptyEtag) {
        return;
      }

      BaseServiceImpl.Enforcers.enforce(meta, precondition, correlationId);
    }
  }
}

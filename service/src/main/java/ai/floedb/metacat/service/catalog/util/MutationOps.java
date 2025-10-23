package ai.floedb.metacat.service.catalog.util;

import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.storage.IdempotencyStore;
import ai.floedb.metacat.service.storage.util.IdempotencyGuard;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
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
}

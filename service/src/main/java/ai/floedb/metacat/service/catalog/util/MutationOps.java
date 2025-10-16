package ai.floedb.metacat.service.catalog.util;

import java.util.function.Function;
import java.util.function.Supplier;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.service.storage.IdempotencyStore;
import ai.floedb.metacat.service.storage.util.IdempotencyGuard;

public final class MutationOps {

  private MutationOps() { }

  public static final class OpResult<T> {
    public final T body;
    public final MutationMeta meta;
    public OpResult(T body, MutationMeta meta) {
      this.body = body; this.meta = meta;
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
      String opName,
      String idemKeyOrEmpty,
      Fingerprinter fp,
      Creator<T> creator,
      Function<T, MutationMeta> metaOf,
      Function<T, byte[]> serializer,
      Function<byte[], T> parser,
      IdempotencyStore idemStore,
      Timestamp now,
      long ttlSeconds,
      Supplier<String> corrIdSupplier) {

    T body = IdempotencyGuard.runOnce(
        tenant,
        opName,
        idemKeyOrEmpty,
        fp.fingerprint(),
        creator::create,
        metaOf,
        serializer,
        parser,
        idemStore,
        ttlSeconds,
        now,
        corrIdSupplier
    );

    return new OpResult<>(body, metaOf.apply(body));
  }

  public static <T extends Message> OpResult<T> createProto(
      String tenant,
      String opName,
      String idemKeyOrEmpty,
      Fingerprinter fp,
      Creator<T> creator,
      Function<T, MutationMeta> metaOf,
      IdempotencyStore idemStore,
      Timestamp now,
      long ttlSeconds,
      Supplier<String> corrIdSupplier,
      ThrowingParser<T> parser) {

    return create(
        tenant, opName, idemKeyOrEmpty, fp, creator, metaOf,
        Message::toByteArray,
        bytes -> {
          try { return parser.parse(bytes); }
          catch (Exception e) { throw new IllegalStateException(e); }
        },
        idemStore, now, ttlSeconds, corrIdSupplier
    );
  }
}

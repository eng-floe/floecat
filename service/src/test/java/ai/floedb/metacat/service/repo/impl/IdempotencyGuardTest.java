package ai.floedb.metacat.service.repo.impl;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.common.IdempotencyGuard;
import ai.floedb.metacat.service.common.MutationOps;
import ai.floedb.metacat.service.repo.IdempotencyRepository;
import ai.floedb.metacat.service.repo.model.Keys;
import ai.floedb.metacat.service.repo.util.BaseResourceRepository;
import ai.floedb.metacat.storage.BlobStore;
import ai.floedb.metacat.storage.InMemoryBlobStore;
import ai.floedb.metacat.storage.InMemoryPointerStore;
import ai.floedb.metacat.storage.PointerStore;
import ai.floedb.metacat.storage.rpc.IdempotencyRecord;
import com.google.protobuf.Any;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IdempotencyGuardTest {

  private static final String TENANT = "t1";
  private static final String OP = "CreateThing";
  private static final Timestamp NOW = ts(Instant.parse("2025-01-01T00:00:00Z"));

  private IdempotencyRepository repo;
  private PointerStore ptr;
  private BlobStore blobs;

  @BeforeEach
  void setUp() {
    ptr = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    repo = new IdempotencyRepositoryImpl(ptr, blobs);
  }

  @Test
  void runOnceEmptyKey() {
    var out =
        IdempotencyGuard.runOnce(
            TENANT,
            OP,
            "",
            "req".getBytes(StandardCharsets.UTF_8),
            () -> new IdempotencyGuard.CreateResult<>("R1", resourceId("rid1")),
            metaOfVersion(1),
            strSer(),
            strParser(),
            repo,
            300,
            NOW,
            () -> "corr");

    assertThat(out).isEqualTo("R1");
  }

  @Test
  void runOnceReplay() throws Exception {
    var creator =
        (Supplier<IdempotencyGuard.CreateResult<String>>)
            () -> new IdempotencyGuard.CreateResult<>("R1", resourceId("rid1"));

    String idemKey = "abc";
    byte[] req = "same".getBytes(StandardCharsets.UTF_8);

    var r1 =
        IdempotencyGuard.runOnce(
            TENANT,
            OP,
            idemKey,
            req,
            creator,
            metaOfVersion(7),
            strSer(),
            strParser(),
            repo,
            300,
            NOW,
            () -> "corr");
    assertThat(r1).isEqualTo("R1");

    assertThat(repo.get(Keys.idempotencyKey(TENANT, OP, idemKey)))
        .get()
        .extracting(IdempotencyRecord::getStatus)
        .isEqualTo(IdempotencyRecord.Status.SUCCEEDED);

    var r2 =
        IdempotencyGuard.runOnce(
            TENANT,
            OP,
            idemKey,
            req,
            () -> new IdempotencyGuard.CreateResult<>("SHOULD_NOT_BE_USED", resourceId("rid2")),
            metaOfVersion(7),
            strSer(),
            strParser(),
            repo,
            300,
            NOW,
            () -> "corr");
    assertThat(r2).isEqualTo("R1");
  }

  @Test
  void runOnceRejectDifferentFingerprint() throws Exception {
    final String idemKey = "k1";
    final byte[] seedReq = "AAA".getBytes(StandardCharsets.UTF_8);
    final String key = Keys.idempotencyKey(TENANT, OP, idemKey);

    MessageDigest md = MessageDigest.getInstance("SHA-256");
    String requestHash = Base64.getEncoder().encodeToString(md.digest(seedReq));

    Timestamp createdAt = NOW;
    Timestamp expiresAt = Timestamps.add(NOW, Duration.newBuilder().setSeconds(300).build());

    boolean pendingOk = repo.createPending(TENANT, key, OP, requestHash, createdAt, expiresAt);
    assertThat(pendingOk).isTrue();

    repo.finalizeSuccess(
        TENANT,
        key,
        OP,
        requestHash,
        ResourceId.newBuilder().setTenantId(TENANT).setId("rid1").build(),
        MutationMeta.newBuilder().setPointerVersion(5).build(),
        "payload1".getBytes(StandardCharsets.UTF_8),
        createdAt,
        expiresAt);

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                IdempotencyGuard.runOnce(
                    TENANT,
                    OP,
                    idemKey,
                    "BBB".getBytes(StandardCharsets.UTF_8),
                    () ->
                        new IdempotencyGuard.CreateResult<>(
                            "R2",
                            ResourceId.newBuilder().setTenantId(TENANT).setId("rid2").build()),
                    s -> MutationMeta.newBuilder().setPointerVersion(9).build(),
                    s -> s.getBytes(StandardCharsets.UTF_8),
                    b -> new String(b, StandardCharsets.UTF_8),
                    repo,
                    300,
                    NOW,
                    () -> "corr"));

    assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);

    var st = StatusProto.fromThrowable(ex);
    assertThat(st).isNotNull();

    ai.floedb.metacat.common.rpc.Error mcError = null;
    for (Any any : st.getDetailsList()) {
      if (any.is(ai.floedb.metacat.common.rpc.Error.class)) {
        mcError = any.unpack(ai.floedb.metacat.common.rpc.Error.class);
        break;
      }
    }
    assertThat(mcError).isNotNull();
    assertThat(mcError.getCode()).isEqualTo(ErrorCode.MC_CONFLICT);
    assertThat(mcError.getMessageKey()).isEqualTo("idempotency_mismatch");
    assertThat(mcError.getParamsMap()).containsEntry("op", OP).containsEntry("key", idemKey);
  }

  @Test
  void runOnceDetectReplayParseFailure() throws Exception {
    String idemKey = "k3";
    byte[] req = "REQ".getBytes(StandardCharsets.UTF_8);

    seedSucceeded(
        TENANT,
        OP,
        idemKey,
        req,
        "OLD".getBytes(StandardCharsets.UTF_8),
        resourceId("rid-old"),
        meta(1));

    var result =
        IdempotencyGuard.runOnce(
            TENANT,
            OP,
            idemKey,
            req,
            () -> new IdempotencyGuard.CreateResult<>("NEW", resourceId("rid-new")),
            metaOfVersion(2),
            strSer(),
            strParser(),
            repo,
            60,
            NOW,
            () -> "corr",
            rec -> false);
    assertThat(result).isEqualTo("NEW");

    var rec = repo.get(Keys.idempotencyKey(TENANT, OP, idemKey)).orElseThrow();
    assertThat(rec.getPayload().toStringUtf8()).isEqualTo("NEW");

    rec = repo.get(Keys.idempotencyKey(TENANT, OP, idemKey)).orElseThrow();
    assertThat(rec.getStatus()).isEqualTo(IdempotencyRecord.Status.SUCCEEDED);
    assertThat(rec.getPayload().toStringUtf8()).isEqualTo("NEW");
  }

  @Test
  void createProto_replayParseError_isWrappedAsCorruptionException() throws Exception {
    String idemKey = "k4";
    byte[] req = "REQ".getBytes(StandardCharsets.UTF_8);

    String key = Keys.idempotencyKey(TENANT, OP, idemKey);
    String requestHash = sha256B64(req);
    repo.createPending(TENANT, key, OP, requestHash, NOW, expiresFrom(NOW, 60));
    repo.finalizeSuccess(
        TENANT,
        key,
        OP,
        requestHash,
        resourceId("X"),
        meta(1),
        "NOT_A_VALID_PROTO".getBytes(StandardCharsets.UTF_8),
        NOW,
        expiresFrom(NOW, 60));

    MutationOps.Creator<ResourceId> creator =
        () -> new IdempotencyGuard.CreateResult<>(resourceId("X"), resourceId("X"));

    assertThatThrownBy(
            () ->
                MutationOps.<ResourceId>createProto(
                    TENANT,
                    OP,
                    idemKey,
                    () -> req,
                    creator,
                    x -> meta(1),
                    repo,
                    NOW,
                    60,
                    () -> "corr",
                    bytes -> {
                      throw new RuntimeException("parse failed");
                    },
                    rec -> true))
        .isInstanceOf(BaseResourceRepository.CorruptionException.class)
        .hasMessageContaining("idempotency_parse_failed");
  }

  @Test
  void runOnceVetoReplayCleanup() throws Exception {
    String idemKey = "k1";
    byte[] req = "REQ".getBytes(StandardCharsets.UTF_8);

    seedSucceeded(
        TENANT,
        OP,
        idemKey,
        req,
        "OLD".getBytes(StandardCharsets.UTF_8),
        resourceId("rid-old"),
        meta(1));

    assertThatThrownBy(
            () ->
                IdempotencyGuard.runOnce(
                    TENANT,
                    OP,
                    idemKey,
                    req,
                    () -> {
                      throw new RuntimeException("failure");
                    },
                    s -> meta(2),
                    s -> s.getBytes(StandardCharsets.UTF_8),
                    b -> new String(b, StandardCharsets.UTF_8),
                    repo,
                    60,
                    NOW,
                    () -> "corr",
                    rec -> false))
        .hasMessageContaining("failure");

    assertThat(repo.get(Keys.idempotencyKey(TENANT, OP, idemKey))).isEmpty();
  }

  private static Function<String, byte[]> strSer() {
    return s -> s.getBytes(StandardCharsets.UTF_8);
  }

  private static Function<byte[], String> strParser() {
    return b -> new String(b, StandardCharsets.UTF_8);
  }

  private static Function<String, MutationMeta> metaOfVersion(long v) {
    return s -> meta(v);
  }

  private static MutationMeta meta(long version) {
    return MutationMeta.newBuilder().setPointerVersion(version).build();
  }

  private static ResourceId resourceId(String id) {
    return ResourceId.newBuilder().setTenantId(TENANT).setId(id).build();
  }

  private static Timestamp ts(Instant i) {
    return Timestamp.newBuilder().setSeconds(i.getEpochSecond()).setNanos(i.getNano()).build();
  }

  private static String sha256B64(byte[] data) throws Exception {
    var md = MessageDigest.getInstance("SHA-256");
    return Base64.getEncoder().encodeToString(md.digest(data));
  }

  private static Timestamp expiresFrom(Timestamp now, long ttlSeconds) {
    return Timestamps.add(now, Duration.newBuilder().setSeconds(ttlSeconds).build());
  }

  private void seedSucceeded(
      String tenant,
      String op,
      String idem,
      byte[] requestBytes,
      byte[] payloadBytes,
      ResourceId rid,
      MutationMeta meta)
      throws Exception {

    String key = Keys.idempotencyKey(tenant, op, idem);
    String requestHash = sha256B64(requestBytes);

    Timestamp createdAt = NOW;
    Timestamp expiresAt = Timestamps.add(NOW, Duration.newBuilder().setSeconds(300).build());

    boolean ok = repo.createPending(tenant, key, op, requestHash, createdAt, expiresAt);
    assertThat(ok).isTrue();

    repo.finalizeSuccess(
        tenant, key, op, requestHash, rid, meta, payloadBytes, createdAt, expiresAt);

    var rec = repo.get(key).orElseThrow();
    assertThat(rec.getStatus()).isEqualTo(IdempotencyRecord.Status.SUCCEEDED);
  }
}

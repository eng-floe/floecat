package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.*;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.gc.IdempotencyGc;
import ai.floedb.floecat.service.it.profiles.GcOnProfile;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(GcOnProfile.class)
class ConcurrencyOCCIdempotencyIT {

  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @Inject IdempotencyGc idemGc;
  @Inject PointerStore ptr;
  @Inject BlobStore blobs;

  final int WORKERS = 48;
  final int OPS_PER_WORKER = 200;

  ExecutorService pool = Executors.newFixedThreadPool(WORKERS);
  CountDownLatch start = new CountDownLatch(1);

  final AtomicBoolean seedDeleted = new AtomicBoolean(false);

  final ConcurrentLinkedQueue<Throwable> unexpected = new ConcurrentLinkedQueue<>();
  final ConcurrentMap<String, LongAdder> expectedCounts = new ConcurrentHashMap<>();

  final ConcurrentMap<String, ResourceId> successfulCreatesByIdem = new ConcurrentHashMap<>();
  final Set<String> createdTableNames = ConcurrentHashMap.newKeySet();
  final Set<ResourceId> createdTableIds = ConcurrentHashMap.newKeySet();

  final List<String> sharedIdemKeys = List.of("alpha", "beta", "gamma");

  enum Op {
    CREATE_IDEMPOTENT,
    UPDATE_SCHEMA,
    RENAME,
    MOVE,
    DELETE_SEED
  }

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void tableConncurrencyTest() throws Exception {
    var catName = "cat_stress_" + System.currentTimeMillis();
    var cat = TestSupport.createCatalog(catalog, catName, "stress");

    var ns1 =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "ns1", List.of("db", "sch"), "ns1");
    var ns2 =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "ns2", List.of("db", "sch"), "ns2");

    var base =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns1.getResourceId(),
            "seed",
            "s3://b/p",
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}",
            "seed");

    var seedTid = base.getResourceId();
    String canonSeed = Keys.tablePointerById(seedTid.getAccountId(), seedTid.getId());
    long v0 = ptr.get(canonSeed).orElseThrow().getVersion();

    int workers =
        Integer.getInteger(
            "floecat.test.idem.workers",
            Math.min(24, Math.max(8, Runtime.getRuntime().availableProcessors() * 2)));
    int opsPerWorker = Integer.getInteger("floecat.test.idem.ops", 8);

    var sharedIdemKeys = List.of("K1", "K2", "K3", "K4");
    var createdTableNames = Collections.newSetFromMap(new ConcurrentHashMap<>());
    var createdTableIds = ConcurrentHashMap.<ResourceId>newKeySet();
    var successfulCreatesByIdem = new ConcurrentHashMap<String, ResourceId>();

    CountDownLatch start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(workers);

    AtomicBoolean seedDeleted = new AtomicBoolean(false);

    for (int w = 0; w < workers; w++) {
      pool.submit(
          () -> {
            try {
              start.await();
              for (int i = 0; i < opsPerWorker; i++) {
                Op op = pickOp();
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                switch (op) {
                  case CREATE_IDEMPOTENT -> {
                    String idemStem = sharedIdemKeys.get(rnd.nextInt(sharedIdemKeys.size()));
                    String display = "idem_" + idemStem;

                    var upstream =
                        UpstreamRef.newBuilder()
                            .setFormat(TableFormat.TF_DELTA)
                            .setUri("s3://b/p")
                            .build();

                    var spec =
                        TableSpec.newBuilder()
                            .setCatalogId(cat.getResourceId())
                            .setNamespaceId(ns1.getResourceId())
                            .setDisplayName(display)
                            .setUpstream(upstream)
                            .setSchemaJson(
                                "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}")
                            .build();
                    var req =
                        CreateTableRequest.newBuilder()
                            .setSpec(spec)
                            .setIdempotency(idem(idemStem))
                            .build();

                    try {
                      var resp = table.createTable(req);
                      var tid = resp.getTable().getResourceId();
                      successfulCreatesByIdem.putIfAbsent(idemStem, tid);
                      createdTableNames.add(display);
                      createdTableIds.add(tid);
                    } catch (Throwable t) {
                      recordOutcome(op, t);
                    }
                  }

                  case UPDATE_SCHEMA -> {
                    if (seedDeleted.get()) break;
                    String col = "c" + rnd.nextInt(100_000);
                    String sj =
                        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},"
                            + "{\"name\":\""
                            + col
                            + "\",\"type\":\"double\"}]}";
                    try {
                      TestSupport.updateSchema(table, seedTid, sj);
                    } catch (Throwable t) {
                      recordOutcome(op, t);
                    }
                  }

                  case RENAME -> {
                    if (seedDeleted.get()) break;
                    String newName = "seed_" + rnd.nextInt(50);
                    try {
                      TestSupport.renameTable(table, seedTid, newName);
                    } catch (Throwable t) {
                      recordOutcome(op, t);
                    }
                  }

                  case MOVE -> {
                    if (seedDeleted.get()) break;
                    try {
                      var targetNs = rnd.nextBoolean() ? ns1.getResourceId() : ns2.getResourceId();
                      FieldMask mask = FieldMask.newBuilder().addPaths("namespace_id").build();
                      TableSpec spec = TableSpec.newBuilder().setNamespaceId(targetNs).build();
                      table.updateTable(
                          UpdateTableRequest.newBuilder()
                              .setTableId(seedTid)
                              .setSpec(spec)
                              .setUpdateMask(mask)
                              .build());
                    } catch (Throwable t) {
                      recordOutcome(op, t);
                    }
                  }

                  case DELETE_SEED -> {
                    if (seedDeleted.compareAndSet(false, true)) {
                      try {
                        table.deleteTable(
                            DeleteTableRequest.newBuilder().setTableId(seedTid).build());
                      } catch (Throwable t) {
                        recordOutcome(op, t);
                      }
                    }
                  }
                }
              }
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              unexpected.add(ie);
            }
          });
    }

    start.countDown();
    pool.shutdown();
    int timeoutSeconds = Integer.getInteger("floecat.test.idem.timeoutSeconds", 120);
    boolean finished = pool.awaitTermination(timeoutSeconds, TimeUnit.SECONDS);
    if (!finished) {
      pool.shutdownNow();
    }
    assertTrue(finished, "workers timed out");

    if (!unexpected.isEmpty()) {
      unexpected.peek().printStackTrace();
    }
    assertTrue(unexpected.isEmpty(), "unexpected errors observed: " + unexpected.size());

    for (String k : sharedIdemKeys) {
      var tid = successfulCreatesByIdem.get(k);
      if (tid == null) {
        continue;
      }
      var ref =
          NameRef.newBuilder()
              .setCatalog(catName)
              .addAllPath(List.of("db", "sch", "ns1"))
              .setName("idem_" + k)
              .build();
      try {
        var resolved = directory.resolveTable(ResolveTableRequest.newBuilder().setRef(ref).build());
        assertEquals(
            tid.getId(),
            resolved.getResourceId().getId(),
            "idempotent create must resolve to same id");
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
          throw e;
        }
      }
    }

    for (ResourceId id : createdTableIds) {
      var canonKey = Keys.tablePointerById(id.getAccountId(), id.getId());
      var ptrRec = ptr.get(canonKey);
      assertTrue(ptrRec.isPresent(), "canon pointer must exist for created table");
      assertTrue(
          blobs.head(ptrRec.orElseThrow().getBlobUri()).isPresent(),
          "blob must exist for created table");
    }

    if (ptr.get(canonSeed).isPresent()) {
      long vSeed = ptr.get(canonSeed).orElseThrow().getVersion();
      assertTrue(vSeed >= v0, "seed pointer version must be >= initial");
    }

    Thread.sleep(1500);

    final String accountId = cat.getResourceId().getAccountId();

    String idemBlobPrefix = Keys.idempotencyPrefixAccount(accountId);
    String idemPtrPrefix = Keys.idempotencyPrefixAccount(accountId);

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    do {
      if (listAllBlobKeysUnder(idemBlobPrefix).isEmpty()
          && listAllPointersUnder(idemPtrPrefix).isEmpty()) break;
      Thread.sleep(200);
    } while (System.nanoTime() < deadline);

    var remainingBlobs = listAllBlobKeysUnder(idemBlobPrefix);
    var remainingPtrs = listAllPointersUnder(idemPtrPrefix);

    assertTrue(remainingBlobs.isEmpty(), "idempotency blobs should be GC'd");
    assertTrue(remainingPtrs.isEmpty(), "idempotency pointers should be GC'd");
  }

  private static IdempotencyKey idem(String s) {
    var key =
        Base64.getUrlEncoder().withoutPadding().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    return IdempotencyKey.newBuilder().setKey(key).build();
  }

  private static Op pickOp() {
    int r = ThreadLocalRandom.current().nextInt(100);
    if (r < 30) return Op.UPDATE_SCHEMA;
    if (r < 50) return Op.RENAME;
    if (r < 70) return Op.MOVE;
    if (r < 90) return Op.CREATE_IDEMPOTENT;
    return Op.DELETE_SEED;
  }

  private static boolean isExpectedGrpcError(Op op, StatusRuntimeException e) {
    var c = e.getStatus().getCode();
    return switch (op) {
      case CREATE_IDEMPOTENT -> c == Status.Code.ABORTED || c == Status.Code.FAILED_PRECONDITION;

      case UPDATE_SCHEMA -> c == Status.Code.FAILED_PRECONDITION || c == Status.Code.NOT_FOUND;

      case RENAME, MOVE ->
          c == Status.Code.FAILED_PRECONDITION
              || c == Status.Code.ABORTED
              || c == Status.Code.NOT_FOUND;

      case DELETE_SEED -> c == Status.Code.FAILED_PRECONDITION || c == Status.Code.NOT_FOUND;
    };
  }

  private void recordOutcome(Op op, Throwable t) {
    if (t instanceof StatusRuntimeException sre) {
      if (isExpectedGrpcError(op, sre)) {
        expectedCounts
            .computeIfAbsent(op + ":" + sre.getStatus().getCode(), k -> new LongAdder())
            .increment();
        return;
      }
    }
    unexpected.add(t);
  }

  private List<String> listAllBlobKeysUnder(String prefix) {
    var out = new ArrayList<String>();
    String tok = "";
    do {
      var page = blobs.list(prefix, 500, tok);
      out.addAll(page.keys());
      tok = page.nextToken() == null ? "" : page.nextToken();
    } while (!tok.isBlank());
    return out;
  }

  private List<Pointer> listAllPointersUnder(String prefix) {
    var out = new ArrayList<ai.floedb.floecat.common.rpc.Pointer>();
    String tok = "";
    StringBuilder next = new StringBuilder();
    do {
      var page = ptr.listPointersByPrefix(prefix, 500, tok, next);
      out.addAll(page);
      tok = next.toString();
      next.setLength(0);
    } while (!tok.isBlank());
    return out;
  }
}

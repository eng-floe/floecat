package ai.floedb.floecat.service.query.graph.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.query.graph.TestNodes;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.storage.InMemoryBlobStore;
import ai.floedb.floecat.storage.InMemoryPointerStore;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnapshotHelperTest {

  private SnapshotHelper helper;
  private FakeSnapshotRepository repository;
  private FakeSnapshotClient snapshotClient;

  @BeforeEach
  void setUp() {
    repository = new FakeSnapshotRepository();
    helper = new SnapshotHelper(repository, null);
    snapshotClient = new FakeSnapshotClient();
    helper.setSnapshotClient(snapshotClient);
  }

  @Test
  void pinUsesExplicitSnapshotId() {
    SnapshotPin pin =
        helper.snapshotPinFor(
            "corr",
            tableId("tbl"),
            SnapshotRef.newBuilder().setSnapshotId(5).build(),
            Optional.empty());

    assertThat(pin.getSnapshotId()).isEqualTo(5);
  }

  @Test
  void pinUsesAsOfOverride() {
    Timestamp ts = ts("2024-01-01T00:00:00Z");
    SnapshotPin pin =
        helper.snapshotPinFor(
            "corr", tableId("tbl"), SnapshotRef.newBuilder().setAsOf(ts).build(), Optional.empty());

    assertThat(pin.getAsOf()).isEqualTo(ts);
    assertThat(pin.getSnapshotId()).isZero();
  }

  @Test
  void pinUsesAsOfDefault() {
    Timestamp ts = ts("2024-02-01T00:00:00Z");
    SnapshotPin pin = helper.snapshotPinFor("corr", tableId("tbl"), null, Optional.of(ts));

    assertThat(pin.getAsOf()).isEqualTo(ts);
  }

  @Test
  void pinFallsBackToSnapshotService() {
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(99L)
            .setUpstreamCreatedAt(ts("2024-03-01T00:00:00Z"))
            .build();
    snapshotClient.nextResponse = GetSnapshotResponse.newBuilder().setSnapshot(snapshot).build();

    SnapshotPin pin = helper.snapshotPinFor("corr", tableId("tbl"), null, Optional.empty());

    assertThat(pin.getSnapshotId()).isEqualTo(99L);
    assertThat(snapshotClient.lastRequest.getSnapshot().getSpecial())
        .isEqualTo(SpecialSnapshot.SS_CURRENT);
  }

  @Test
  void schemaJsonUsesSnapshotPayload() {
    ResourceId tableId = tableId("tbl");
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(41L)
            .setSchemaJson("{\"fields\":[]}")
            .setUpstreamCreatedAt(ts("2024-04-01T00:00:00Z"))
            .build();
    repository.put(tableId, snapshot);

    String schema =
        helper.schemaJsonFor(
            "corr",
            TestNodes.tableNode(tableId, "{}"),
            SnapshotRef.newBuilder().setSnapshotId(41L).build(),
            () -> "{}");

    assertThat(schema).contains("fields");
  }

  @Test
  void schemaJsonThrowsWhenSnapshotMissing() {
    ResourceId tableId = tableId("tbl");
    assertThatThrownBy(
            () ->
                helper.schemaJsonFor(
                    "corr",
                    TestNodes.tableNode(tableId, "{}"),
                    SnapshotRef.newBuilder().setSnapshotId(1L).build(),
                    () -> "{}"))
        .isInstanceOf(RuntimeException.class);
  }

  private static ResourceId tableId(String id) {
    return ResourceId.newBuilder()
        .setAccountId("account")
        .setId(id)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }

  private static Timestamp ts(String instant) {
    Instant parsed = Instant.parse(instant);
    return Timestamp.newBuilder()
        .setSeconds(parsed.getEpochSecond())
        .setNanos(parsed.getNano())
        .build();
  }

  static final class FakeSnapshotRepository extends SnapshotRepository {
    private final Map<Long, Snapshot> snapshots = new HashMap<>();

    FakeSnapshotRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    void put(ResourceId tableId, Snapshot snapshot) {
      snapshots.put(snapshot.getSnapshotId(), snapshot);
    }

    @Override
    public Optional<Snapshot> getById(ResourceId tableId, long snapshotId) {
      return Optional.ofNullable(snapshots.get(snapshotId));
    }

    @Override
    public Optional<Snapshot> getCurrentSnapshot(ResourceId tableId) {
      return snapshots.values().stream().max(Comparator.comparingLong(Snapshot::getSnapshotId));
    }

    @Override
    public Optional<Snapshot> getAsOf(ResourceId tableId, Timestamp asOf) {
      long target = asOf.getSeconds();
      return snapshots.values().stream()
          .filter(s -> s.getUpstreamCreatedAt().getSeconds() <= target)
          .max(Comparator.comparingLong(Snapshot::getSnapshotId));
    }
  }

  static final class FakeSnapshotClient implements SnapshotHelper.SnapshotClient {
    GetSnapshotResponse nextResponse;
    GetSnapshotRequest lastRequest;

    @Override
    public GetSnapshotResponse getSnapshot(GetSnapshotRequest request) {
      lastRequest = request;
      if (nextResponse == null) {
        throw new IllegalStateException("no snapshot configured");
      }
      return nextResponse;
    }
  }
}

package ai.floedb.metacat.service.query.resolve;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.query.rpc.*;
import ai.floedb.metacat.service.query.impl.QueryContext;
import com.google.protobuf.Timestamp;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SnapshotResolver}.
 *
 * <p>This resolver consumes a pre-built QueryContext (with pinned snapshots) and applies final
 * override rules.
 */
public class SnapshotResolverTest {

  SnapshotResolver resolver;

  PrincipalContext principal =
      PrincipalContext.newBuilder().setTenantId("T").setSubject("user").build();

  @BeforeEach
  void init() {
    resolver = new SnapshotResolver();
  }

  private ResourceId table(String id) {
    return ResourceId.newBuilder().setId(id).setKind(ResourceKind.RK_TABLE).build();
  }

  private ResourceId view(String id) {
    return ResourceId.newBuilder().setId(id).setKind(ResourceKind.RK_VIEW).build();
  }

  /** Builds a QueryContext with given pinned snapshots. */
  private QueryContext ctxWithPins(SnapshotPin... pins) {
    SnapshotSet set = SnapshotSet.newBuilder().addAllPins(Arrays.asList(pins)).build();

    return QueryContext.builder()
        .queryId("Q1")
        .principal(principal)
        .snapshotSet(set.toByteArray())
        .expansionMap(null)
        .obligations(null)
        .asOfDefault(null)
        .createdAtMs(1)
        .expiresAtMs(10)
        .state(QueryContext.State.ACTIVE)
        .version(1)
        .build();
  }

  // =======================================================================
  // Tests
  // =======================================================================

  /** Explicit snapshotId override must win over pinned snapshot. */
  @Test
  void snapshotId_override_wins() {
    ResourceId t = table("T1");

    QueryContext ctx =
        ctxWithPins(SnapshotPin.newBuilder().setTableId(t).setSnapshotId(111).build());

    QueryInput qi =
        QueryInput.newBuilder()
            .setTableId(t)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(999))
            .build();

    SnapshotPin out = resolver.resolvePins("cid", ctx, List.of(qi)).get(0);

    assertEquals(999, out.getSnapshotId());
    assertFalse(out.hasAsOf());
  }

  /** Explicit as-of override must win over pinned snapshot. */
  @Test
  void asof_override_wins() {
    ResourceId t = table("T2");

    QueryContext ctx =
        ctxWithPins(SnapshotPin.newBuilder().setTableId(t).setSnapshotId(111).build());

    Timestamp ts = Timestamp.newBuilder().setSeconds(777).build();

    QueryInput qi =
        QueryInput.newBuilder()
            .setTableId(t)
            .setSnapshot(SnapshotRef.newBuilder().setAsOf(ts))
            .build();

    SnapshotPin out = resolver.resolvePins("cid", ctx, List.of(qi)).get(0);

    assertEquals(ts, out.getAsOf());
    assertEquals(0L, out.getSnapshotId());
  }

  /** Falls back to pinned snapshot when no override exists. */
  @Test
  void fallback_to_pinned_snapshot() {
    ResourceId t = table("T3");

    QueryContext ctx =
        ctxWithPins(SnapshotPin.newBuilder().setTableId(t).setSnapshotId(4444).build());

    QueryInput qi = QueryInput.newBuilder().setTableId(t).build();

    SnapshotPin out = resolver.resolvePins("cid", ctx, List.of(qi)).get(0);

    assertEquals(4444, out.getSnapshotId());
  }

  /** Views produce empty pins with no snapshot. */
  @Test
  void view_produces_empty_pin() {
    ResourceId v = view("V1");

    QueryContext ctx = ctxWithPins(); // no pins needed

    QueryInput qi = QueryInput.newBuilder().setViewId(v).build();

    SnapshotPin out = resolver.resolvePins("cid", ctx, List.of(qi)).get(0);

    assertEquals("V1", out.getTableId().getId());
    assertFalse(out.hasAsOf());
    assertEquals(0L, out.getSnapshotId());
  }

  /** Inputs must have either tableId or viewId. */
  @Test
  void invalid_input_type_throws() {
    QueryContext ctx = ctxWithPins();

    QueryInput qi = QueryInput.newBuilder().build(); // invalid

    assertThrows(RuntimeException.class, () -> resolver.resolvePins("cid", ctx, List.of(qi)));
  }

  /** Multiple inputs must remain independent. */
  @Test
  void multiple_inputs_independent() {
    ResourceId t1 = table("T10");
    ResourceId t2 = table("T20");

    QueryContext ctx =
        ctxWithPins(
            SnapshotPin.newBuilder().setTableId(t1).setSnapshotId(1).build(),
            SnapshotPin.newBuilder().setTableId(t2).setSnapshotId(2).build());

    QueryInput q1 =
        QueryInput.newBuilder()
            .setTableId(t1)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(999))
            .build();

    QueryInput q2 = QueryInput.newBuilder().setTableId(t2).build();

    List<SnapshotPin> out = resolver.resolvePins("cid", ctx, List.of(q1, q2));

    assertEquals(999, out.get(0).getSnapshotId()); // override
    assertEquals(2, out.get(1).getSnapshotId()); // pinned fallback
  }

  /** Override in one input must not affect other inputs. */
  @Test
  void override_does_not_affect_other_input() {
    ResourceId t1 = table("T30");
    ResourceId t2 = table("T40");

    QueryContext ctx =
        ctxWithPins(
            SnapshotPin.newBuilder().setTableId(t1).setSnapshotId(5).build(),
            SnapshotPin.newBuilder().setTableId(t2).setSnapshotId(6).build());

    Timestamp ts = Timestamp.newBuilder().setSeconds(500).build();

    QueryInput q1 =
        QueryInput.newBuilder()
            .setTableId(t1)
            .setSnapshot(SnapshotRef.newBuilder().setAsOf(ts))
            .build();

    QueryInput q2 = QueryInput.newBuilder().setTableId(t2).build();

    List<SnapshotPin> out = resolver.resolvePins("cid", ctx, List.of(q1, q2));

    assertEquals(ts, out.get(0).getAsOf());
    assertEquals(6, out.get(1).getSnapshotId());
  }
}

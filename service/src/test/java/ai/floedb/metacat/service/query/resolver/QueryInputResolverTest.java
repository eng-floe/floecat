package ai.floedb.metacat.service.query.resolve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.query.rpc.QueryInput;
import ai.floedb.metacat.query.rpc.SnapshotPin;
import ai.floedb.metacat.service.query.graph.MetadataGraph;
import com.google.protobuf.Timestamp;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QueryInputResolverTest {

  QueryInputResolver resolver;
  FakeGraph metadataGraph;

  @BeforeEach
  void init() {
    resolver = new QueryInputResolver();
    metadataGraph = new FakeGraph();
    resolver.metadataGraph = metadataGraph;
  }

  NameRef name(String cat, String... parts) {
    NameRef.Builder b = NameRef.newBuilder().setCatalog(cat);
    for (int i = 0; i < parts.length - 1; i++) {
      b.addPath(parts[i]);
    }
    b.setName(parts[parts.length - 1]);
    return b.build();
  }

  ResourceId rid(String id) {
    return ResourceId.newBuilder().setId(id).build();
  }

  /** Resolving a name that maps only to a table should return the table id. */
  @Test
  void resolve_table_only() {
    NameRef n = name("c", "ns", "t");
    ResourceId tableId = rid("T1");
    metadataGraph.bind(n, tableId);

    var res =
        resolver.resolveInputs(
            "cid", List.of(QueryInput.newBuilder().setName(n).build()), Optional.empty());

    assertEquals("T1", res.resolved().get(0).getId());
  }

  /** Resolving a name that maps only to a view should return the view id. */
  @Test
  void resolve_view_only() {
    NameRef n = name("c", "ns", "v");
    ResourceId viewId = ResourceId.newBuilder().setId("V1").setKind(ResourceKind.RK_VIEW).build();
    metadataGraph.bind(n, viewId);

    var res =
        resolver.resolveInputs(
            "cid", List.of(QueryInput.newBuilder().setName(n).build()), Optional.empty());

    assertEquals("V1", res.resolved().get(0).getId());
  }

  /** If both table and view match the same name, resolution is ambiguous and must fail. */
  @Test
  void resolve_ambiguous() {
    NameRef n = name("c", "x", "y");
    metadataGraph.fail(n, new StatusRuntimeException(io.grpc.Status.INVALID_ARGUMENT));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            resolver.resolveInputs(
                "cid", List.of(QueryInput.newBuilder().setName(n).build()), Optional.empty()));
  }

  /** Name resolution should fail if no table or view exists for the given NameRef. */
  @Test
  void resolve_unresolved() {
    NameRef n = name("c", "a", "b");
    metadataGraph.fail(n, new StatusRuntimeException(io.grpc.Status.INVALID_ARGUMENT));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            resolver.resolveInputs(
                "cid", List.of(QueryInput.newBuilder().setName(n).build()), Optional.empty()));
  }

  /** When a QueryInput specifies a snapshot_id override, the resolver must use it verbatim. */
  @Test
  void snapshot_override_id() {
    NameRef n = name("c", "ns", "t2");
    ResourceId tableId = rid("T2");
    metadataGraph.bind(n, tableId);

    QueryInput qi =
        QueryInput.newBuilder()
            .setName(n)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(777))
            .build();

    SnapshotPin p =
        resolver.resolveInputs("cid", List.of(qi), Optional.empty()).snapshotSet().getPins(0);

    assertEquals("T2", p.getTableId().getId());
    assertEquals(777, p.getSnapshotId());
    FakeGraph.PinCall call = metadataGraph.pinCalls().get(metadataGraph.pinCalls().size() - 1);
    assertEquals(tableId, call.tableId());
    assertEquals(777, call.override().getSnapshotId());
    assertTrue(call.asOfDefault().isEmpty());
  }

  /**
   * When a QueryInput specifies an explicit AS OF timestamp, the resolver must interpret it as a
   * timestamp snapshot (snapshotId = 0, asOf set).
   */
  @Test
  void snapshot_override_asof() {
    NameRef n = name("c", "ns", "t3");
    ResourceId tableId = rid("T3");
    metadataGraph.bind(n, tableId);
    Timestamp ts = Timestamp.newBuilder().setSeconds(100).build();

    QueryInput qi =
        QueryInput.newBuilder()
            .setName(n)
            .setSnapshot(SnapshotRef.newBuilder().setAsOf(ts))
            .build();

    SnapshotPin p =
        resolver.resolveInputs("cid", List.of(qi), Optional.empty()).snapshotSet().getPins(0);

    assertEquals(0L, p.getSnapshotId());
    assertEquals(ts, p.getAsOf());
    FakeGraph.PinCall call = metadataGraph.pinCalls().get(metadataGraph.pinCalls().size() - 1);
    assertEquals(Optional.empty(), call.asOfDefault());
    assertEquals(ts, call.override().getAsOf());
  }

  /**
   * If no snapshot override is given but an as-of-default is provided, the resolver must use a
   * timestamp-based pin.
   */
  @Test
  void snapshot_asof_default() {
    NameRef n = name("c", "ns", "t4");
    ResourceId tableId = rid("T4");
    metadataGraph.bind(n, tableId);

    Timestamp ts = Timestamp.newBuilder().setSeconds(50).build();

    SnapshotPin p =
        resolver
            .resolveInputs(
                "cid", List.of(QueryInput.newBuilder().setName(n).build()), Optional.of(ts))
            .snapshotSet()
            .getPins(0);

    assertEquals(0L, p.getSnapshotId());
    assertEquals(ts, p.getAsOf());
    FakeGraph.PinCall call = metadataGraph.pinCalls().get(metadataGraph.pinCalls().size() - 1);
    assertEquals(Optional.of(ts), call.asOfDefault());
    assertEquals(SnapshotRef.WhichCase.WHICH_NOT_SET, call.override().getWhichCase());
  }

  /**
   * With no overrides and no as-of-default, the resolver must fall back to the CURRENT snapshot for
   * the table.
   */
  @Test
  void snapshot_fallback_current() {
    NameRef n = name("c", "ns", "t5");
    ResourceId tableId = rid("T5");
    metadataGraph.bind(n, tableId);
    metadataGraph.setCurrentSnapshot(tableId, 4444L);

    SnapshotPin p =
        resolver
            .resolveInputs(
                "cid", List.of(QueryInput.newBuilder().setName(n).build()), Optional.empty())
            .snapshotSet()
            .getPins(0);

    assertEquals(4444, p.getSnapshotId());
  }

  /**
   * When specifying a direct tableId instead of a name, snapshot resolution should behave
   * identically to using a NameRef that resolves to the same ResourceId.
   */
  @Test
  void direct_table_id() {
    ResourceId rid = rid("TABX");
    metadataGraph.setCurrentSnapshot(rid, 222L);

    SnapshotPin p =
        resolver
            .resolveInputs(
                "cid", List.of(QueryInput.newBuilder().setTableId(rid).build()), Optional.empty())
            .snapshotSet()
            .getPins(0);

    assertEquals("TABX", p.getTableId().getId());
    assertEquals(222, p.getSnapshotId());
  }

  /** Views never have snapshots. A viewId must always produce a pin with snapshotId=0. */
  @Test
  void direct_view_id() {
    ResourceId rid = rid("VIEWX");

    SnapshotPin p =
        resolver
            .resolveInputs(
                "cid", List.of(QueryInput.newBuilder().setViewId(rid).build()), Optional.empty())
            .snapshotSet()
            .getPins(0);

    assertEquals("VIEWX", p.getTableId().getId());
    assertEquals(0L, p.getSnapshotId());
  }

  /**
   * Protobuf oneof rule: the last field set wins. Setting snapshot_id and then as_of results in a
   * SnapshotRef that contains only as_of.
   */
  @Test
  void snapshot_override_last_field_wins() {
    NameRef n = name("c", "ns", "t6");
    ResourceId tableId = rid("T6");
    metadataGraph.bind(n, tableId);

    Timestamp ts = Timestamp.newBuilder().setSeconds(999).build();

    SnapshotRef ref =
        SnapshotRef.newBuilder().setSnapshotId(555).setAsOf(ts).build(); // last field wins

    assertEquals(SnapshotRef.WhichCase.AS_OF, ref.getWhichCase());
    assertFalse(ref.hasSnapshotId());
    assertEquals(ts, ref.getAsOf());

    QueryInput qi = QueryInput.newBuilder().setName(n).setSnapshot(ref).build();

    SnapshotPin p =
        resolver.resolveInputs("cid", List.of(qi), Optional.empty()).snapshotSet().getPins(0);

    assertEquals(0L, p.getSnapshotId());
    assertEquals(ts, p.getAsOf());
  }

  /**
   * Multiple inputs should be resolved independently and results must preserve input order. This
   * ensures stable output ordering, which callers rely on.
   */
  @Test
  void multiple_inputs_resolve_in_order() {
    NameRef n1 = name("c", "x", "t1");
    NameRef n2 = name("c", "y", "t2");

    ResourceId r1 = rid("T1");
    ResourceId r2 = rid("T2");
    metadataGraph.bind(n1, r1);
    metadataGraph.bind(n2, r2);
    metadataGraph.setCurrentSnapshot(r1, 10L);
    metadataGraph.setCurrentSnapshot(r2, 20L);

    var res =
        resolver.resolveInputs(
            "cid",
            List.of(
                QueryInput.newBuilder().setName(n1).build(),
                QueryInput.newBuilder().setName(n2).build()),
            Optional.empty());

    assertEquals(List.of("T1", "T2"), res.resolved().stream().map(ResourceId::getId).toList());
    assertEquals(10, res.snapshotSet().getPins(0).getSnapshotId());
    assertEquals(20, res.snapshotSet().getPins(1).getSnapshotId());
  }

  /** NameRef with multiple nested path components must resolve correctly. */
  @Test
  void resolve_nested_paths() {
    NameRef n = name("catA", "lvl1", "lvl2", "tbl");
    ResourceId nested = rid("NESTED");
    metadataGraph.bind(n, nested);

    var res =
        resolver.resolveInputs(
            "cid", List.of(QueryInput.newBuilder().setName(n).build()), Optional.empty());

    assertEquals("NESTED", res.resolved().get(0).getId());
  }

  /**
   * NameRef containing a ResourceId must short-circuit p2 resolution logic. i.e.
   * directory.resolveTable/resolveView MUST NOT be called.
   */
  @Test
  void resolve_name_with_explicit_resource_id() {
    ResourceId rid = rid("DIRECT");
    NameRef n =
        NameRef.newBuilder()
            .setCatalog("c")
            .addPath("ns")
            .setName("tbl")
            .setResourceId(rid)
            .build();

    var res =
        resolver.resolveInputs(
            "cid", List.of(QueryInput.newBuilder().setName(n).build()), Optional.empty());

    assertEquals("DIRECT", res.resolved().get(0).getId());
  }

  /** Empty snapshot ref should behave like "no override" and fall back to CURRENT. */
  @Test
  void snapshot_empty_snapshotref_behaves_as_no_override() {
    NameRef n = name("c", "ns", "tbl");
    ResourceId tableId = rid("T_EMPTY");
    metadataGraph.bind(n, tableId);
    metadataGraph.setCurrentSnapshot(tableId, 999L);

    QueryInput qi =
        QueryInput.newBuilder().setName(n).setSnapshot(SnapshotRef.newBuilder().build()).build();

    SnapshotPin pin =
        resolver.resolveInputs("cid", List.of(qi), Optional.empty()).snapshotSet().getPins(0);

    assertEquals(999, pin.getSnapshotId());
  }

  /**
   * Test that an input mixing tableId and name resolution behaves as expected when combined in the
   * same request list.
   */
  @Test
  void mixed_tableid_and_name_inputs() {
    NameRef n = name("c", "x", "tblA");
    ResourceId ridB = rid("tblB");

    ResourceId ridA = rid("tblA");
    metadataGraph.bind(n, ridA);
    metadataGraph.setCurrentSnapshot(ridA, 100L);
    metadataGraph.setCurrentSnapshot(ridB, 200L);

    var res =
        resolver.resolveInputs(
            "cid",
            List.of(
                QueryInput.newBuilder().setName(n).build(),
                QueryInput.newBuilder().setTableId(ridB).build()),
            Optional.empty());

    assertEquals(List.of("tblA", "tblB"), res.resolved().stream().map(ResourceId::getId).toList());
    assertEquals(100L, res.snapshotSet().getPins(0).getSnapshotId());
    assertEquals(200L, res.snapshotSet().getPins(1).getSnapshotId());
  }

  /**
   * If a builder sets no target (no table, no view, no name), resolver must throw INVALID_ARGUMENT.
   */
  @Test
  void missing_target_case_must_error() {
    QueryInput qi = QueryInput.newBuilder().build();

    assertThrows(
        StatusRuntimeException.class,
        () -> resolver.resolveInputs("cid", List.of(qi), Optional.empty()));
  }

  /**
   * A view with an explicit AS OF override is legal but should still produce snapshotId=0, because
   * views do not support storage snapshots.
   */
  @Test
  void view_with_asof_override_uses_timestamp() {
    ResourceId rid = rid("V1");
    Timestamp ts = Timestamp.newBuilder().setSeconds(202).build();

    QueryInput qi =
        QueryInput.newBuilder()
            .setViewId(rid)
            .setSnapshot(SnapshotRef.newBuilder().setAsOf(ts))
            .build();

    SnapshotPin pin =
        resolver.resolveInputs("cid", List.of(qi), Optional.empty()).snapshotSet().getPins(0);

    assertEquals("V1", pin.getTableId().getId());
    assertEquals(0, pin.getSnapshotId());
    assertEquals(ts, pin.getAsOf());
  }

  // ----------------------------------------------------------------------
  // Helpers / test doubles
  // ----------------------------------------------------------------------

  static final class FakeGraph extends MetadataGraph {

    private final Map<NameRef, ResourceId> nameBindings = new HashMap<>();
    private final Map<NameRef, RuntimeException> failures = new HashMap<>();
    private final Map<String, Long> currentSnapshots = new HashMap<>();
    private final List<PinCall> pinCalls = new ArrayList<>();

    FakeGraph() {
      super(null, null, null, null);
    }

    void bind(NameRef ref, ResourceId id) {
      nameBindings.put(ref, id);
    }

    void fail(NameRef ref, RuntimeException ex) {
      failures.put(ref, ex);
    }

    void setCurrentSnapshot(ResourceId id, long snapshotId) {
      currentSnapshots.put(id.getId(), snapshotId);
    }

    List<PinCall> pinCalls() {
      return pinCalls;
    }

    @Override
    public ResourceId resolveName(String correlationId, NameRef ref) {
      if (ref.hasResourceId()) {
        return ref.getResourceId();
      }
      RuntimeException failure = failures.get(ref);
      if (failure != null) {
        throw failure;
      }
      ResourceId id = nameBindings.get(ref);
      if (id == null) {
        throw new StatusRuntimeException(io.grpc.Status.INVALID_ARGUMENT);
      }
      return id;
    }

    @Override
    public SnapshotPin snapshotPinFor(
        String correlationId,
        ResourceId tableId,
        SnapshotRef override,
        Optional<Timestamp> asOfDefault) {
      pinCalls.add(new PinCall(correlationId, tableId, override, asOfDefault));
      SnapshotPin.Builder builder = SnapshotPin.newBuilder().setTableId(tableId);
      if (override != null && override.hasSnapshotId()) {
        builder.setSnapshotId(override.getSnapshotId());
      } else if (override != null && override.hasAsOf()) {
        builder.setAsOf(override.getAsOf());
      } else if (asOfDefault.isPresent()) {
        builder.setAsOf(asOfDefault.get());
      } else {
        long snapshot = currentSnapshots.getOrDefault(tableId.getId(), 0L);
        if (snapshot > 0) {
          builder.setSnapshotId(snapshot);
        }
      }
      return builder.build();
    }

    record PinCall(
        String correlationId,
        ResourceId tableId,
        SnapshotRef override,
        Optional<Timestamp> asOfDefault) {}
  }
}

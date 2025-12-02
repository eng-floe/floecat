package ai.floedb.metacat.service.query.resolver;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.*;
import ai.floedb.metacat.query.rpc.*;
import com.google.protobuf.Timestamp;
import io.grpc.StatusRuntimeException;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QueryInputResolverTest {

  QueryInputResolver resolver;
  FakeDir dir;
  FakeSnaps snaps;

  @BeforeEach
  void init() {
    resolver = new QueryInputResolver();
    dir = new FakeDir();
    snaps = new FakeSnaps();

    resolver.setDirectoryApi(dir);
    resolver.setSnapshotApi(snaps);
  }

  NameRef name(String cat, String... parts) {
    NameRef.Builder b = NameRef.newBuilder().setCatalog(cat);
    for (int i = 0; i < parts.length - 1; i++) b.addPath(parts[i]);
    b.setName(parts[parts.length - 1]);
    return b.build();
  }

  ResourceId rid(String id) {
    return ResourceId.newBuilder().setId(id).build();
  }

  // -------------------------------------------------------
  // TESTS
  // -------------------------------------------------------

  /** Resolving a name that maps only to a table should return the table id. */
  @Test
  void resolve_table_only() {
    NameRef n = name("c", "ns", "t");
    dir.tables.put(n, rid("T1"));

    var res =
        resolver.resolveInputs(
            "cid", List.of(QueryInput.newBuilder().setName(n).build()), Optional.empty());

    assertEquals("T1", res.resolved().get(0).getId());
  }

  /** Resolving a name that maps only to a view should return the view id. */
  @Test
  void resolve_view_only() {
    NameRef n = name("c", "ns", "v");
    dir.views.put(n, rid("V1"));

    var res =
        resolver.resolveInputs(
            "cid", List.of(QueryInput.newBuilder().setName(n).build()), Optional.empty());

    assertEquals("V1", res.resolved().get(0).getId());
  }

  /** If both table and view match the same name, resolution is ambiguous and must fail. */
  @Test
  void resolve_ambiguous() {
    NameRef n = name("c", "x", "y");
    dir.tables.put(n, rid("T"));
    dir.views.put(n, rid("V"));

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
    dir.tables.put(n, rid("T2"));

    QueryInput qi =
        QueryInput.newBuilder()
            .setName(n)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(777))
            .build();

    SnapshotPin p =
        resolver.resolveInputs("cid", List.of(qi), Optional.empty()).snapshotSet().getPins(0);

    assertEquals("T2", p.getTableId().getId());
    assertEquals(777, p.getSnapshotId());
  }

  /**
   * When a QueryInput specifies an explicit AS OF timestamp, the resolver must interpret it as a
   * timestamp snapshot (snapshotId = 0, asOf set).
   */
  @Test
  void snapshot_override_asof() {
    NameRef n = name("c", "ns", "t3");
    dir.tables.put(n, rid("T3"));

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
  }

  /**
   * If no snapshot override is given but an as-of-default is provided, the resolver must use a
   * timestamp-based pin.
   */
  @Test
  void snapshot_asof_default() {
    NameRef n = name("c", "ns", "t4");
    dir.tables.put(n, rid("T4"));
    snaps.current.put("T4", 9999L);

    Timestamp ts = Timestamp.newBuilder().setSeconds(50).build();

    SnapshotPin p =
        resolver
            .resolveInputs(
                "cid", List.of(QueryInput.newBuilder().setName(n).build()), Optional.of(ts))
            .snapshotSet()
            .getPins(0);

    assertEquals(0L, p.getSnapshotId());
    assertEquals(ts, p.getAsOf());
  }

  /**
   * With no overrides and no as-of-default, the resolver must fall back to the CURRENT snapshot for
   * the table.
   */
  @Test
  void snapshot_fallback_current() {
    NameRef n = name("c", "ns", "t5");
    dir.tables.put(n, rid("T5"));
    snaps.current.put("T5", 4444L);

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
    snaps.current.put("TABX", 222L);

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
    dir.tables.put(n, rid("T6"));

    Timestamp ts = Timestamp.newBuilder().setSeconds(999).build();

    SnapshotRef ref =
        SnapshotRef.newBuilder()
            .setSnapshotId(555) // first
            .setAsOf(ts) // last → wins
            .build();

    // Protobuf semantics
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

    dir.tables.put(n1, rid("T1"));
    dir.tables.put(n2, rid("T2"));
    snaps.current.put("T1", 10L);
    snaps.current.put("T2", 20L);

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
    dir.tables.put(n, rid("NESTED"));

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

    // Put impossible mappings to ensure resolver does NOT call directory
    dir.tables.clear();
    dir.views.clear();

    var res =
        resolver.resolveInputs(
            "cid", List.of(QueryInput.newBuilder().setName(n).build()), Optional.empty());

    assertEquals("DIRECT", res.resolved().get(0).getId());
  }

  /** Empty snapshot ref should behave like "no override" and fall back to CURRENT. */
  @Test
  void snapshot_empty_snapshotref_behaves_as_no_override() {
    NameRef n = name("c", "ns", "tbl");
    dir.tables.put(n, rid("T_EMPTY"));
    snaps.current.put("T_EMPTY", 999L);

    // SnapshotRef.newBuilder().build() → which = NONE
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

    dir.tables.put(n, rid("tblA"));
    snaps.current.put("tblA", 100L);
    snaps.current.put("tblB", 200L);

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
    QueryInput qi = QueryInput.newBuilder().build(); // no target set

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

  // -------------------------------------------------------
  // Fake implementations
  // -------------------------------------------------------

  static class FakeDir implements QueryInputResolver.DirectoryApi {

    final Map<NameRef, ResourceId> tables = new HashMap<>();
    final Map<NameRef, ResourceId> views = new HashMap<>();

    public ResolveTableResponse resolveTable(ResolveTableRequest req) {
      ResourceId r = tables.get(req.getRef());
      if (r == null) throw new RuntimeException("no table");
      return ResolveTableResponse.newBuilder().setResourceId(r).build();
    }

    public ResolveViewResponse resolveView(ResolveViewRequest req) {
      ResourceId r = views.get(req.getRef());
      if (r == null) throw new RuntimeException("no view");
      return ResolveViewResponse.newBuilder().setResourceId(r).build();
    }
  }

  static class FakeSnaps implements QueryInputResolver.SnapshotApi {

    final Map<String, Long> current = new HashMap<>();

    public GetSnapshotResponse getSnapshot(GetSnapshotRequest req) {
      long snap = current.getOrDefault(req.getTableId().getId(), 0L);
      Snapshot s = Snapshot.newBuilder().setSnapshotId(snap).build();
      return GetSnapshotResponse.newBuilder().setSnapshot(s).build();
    }
  }
}

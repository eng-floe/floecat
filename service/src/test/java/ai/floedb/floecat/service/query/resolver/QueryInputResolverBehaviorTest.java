/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package ai.floedb.floecat.service.query.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.query.rpc.PinKind;
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport.FakeCatalogGraphView;
import com.google.protobuf.Timestamp;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Contract coverage for input resolution after snapshot selection stopped being a GC root. */
class QueryInputResolverBehaviorTest {
  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("table")
          .setKind(ResourceKind.RK_TABLE)
          .build();
  private static final ResourceId OTHER_TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("other")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private FakeCatalogGraphView graph;
  private QueryInputResolver resolver;

  @BeforeEach
  void setUp() {
    graph = new FakeCatalogGraphView();
    graph.registerTable(TABLE, List.of(), name("cat", "table"));
    graph.registerTable(OTHER_TABLE, List.of(), name("cat", "other"));
    resolver = new QueryInputResolver(graph);
  }

  @Test
  void resolvesNamesAndDirectIdsInRequestOrder() {
    var result =
        resolver.resolveInputs(
            "cid",
            List.of(
                QueryInput.newBuilder().setName(name("cat", "table")).build(),
                QueryInput.newBuilder().setTableId(OTHER_TABLE).build()),
            Optional.empty(),
            Optional.empty());

    assertThat(result.resolved()).containsExactly(TABLE, OTHER_TABLE);
    assertThat(result.relationPinSet().getPinsCount()).isEqualTo(2);
  }

  @Test
  void repeatedCurrentReferencesShareOneResolvedSelection() {
    var input = QueryInput.newBuilder().setTableId(TABLE).build();

    var result =
        resolver.resolveInputs("cid", List.of(input, input), Optional.empty(), Optional.empty());

    assertThat(result.resolved()).containsExactly(TABLE, TABLE);
    assertThat(result.relationPinSet().getPinsCount()).isEqualTo(1);
  }

  @Test
  void explicitSnapshotAndAsOfSelectionsPreserveTheirResolvedProvenance() {
    Timestamp asOf = Timestamp.newBuilder().setSeconds(1_700_000_000L).build();
    TablePin explicit =
        resolve(
                QueryInput.newBuilder()
                    .setTableId(TABLE)
                    .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(7))
                    .build())
            .getPins(0)
            .getTablePin();
    TablePin asOfPin =
        resolve(
                QueryInput.newBuilder()
                    .setTableId(OTHER_TABLE)
                    .setSnapshot(SnapshotRef.newBuilder().setAsOf(asOf))
                    .build())
            .getPins(0)
            .getTablePin();

    assertThat(explicit.getPinKind()).isEqualTo(PinKind.PIN_KIND_SNAPSHOT_ID);
    assertThat(explicit.getSnapshotId()).isEqualTo(7);
    assertThat(asOfPin.getPinKind()).isEqualTo(PinKind.PIN_KIND_AS_OF);
    assertThat(asOfPin.getOriginalAsOf()).isEqualTo(asOf);
  }

  @Test
  void currentOverrideTakesPrecedenceOverAsOfDefault() {
    Timestamp asOf = Timestamp.newBuilder().setSeconds(1_700_000_000L).build();
    TablePin pin =
        resolve(
                QueryInput.newBuilder()
                    .setTableId(TABLE)
                    .setSnapshot(
                        SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build())
                    .build(),
                Optional.of(asOf))
            .getPins(0)
            .getTablePin();

    assertThat(pin.getPinKind()).isEqualTo(PinKind.PIN_KIND_CURRENT);
    assertThat(pin.hasOriginalAsOf()).isFalse();
  }

  @Test
  void incompatibleSelectionsForOneTableFailInsteadOfSilentlyChangingSnapshot() {
    QueryInput first =
        QueryInput.newBuilder()
            .setTableId(TABLE)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(7))
            .build();
    QueryInput second =
        QueryInput.newBuilder()
            .setTableId(TABLE)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(8))
            .build();

    assertThatThrownBy(
            () ->
                resolver.resolveInputs(
                    "cid", List.of(first, second), Optional.empty(), Optional.empty()))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void unresolvedNameFailsWithoutCreatingASelection() {
    QueryInput missing = QueryInput.newBuilder().setName(name("cat", "missing")).build();

    assertThatThrownBy(
            () ->
                resolver.resolveInputs("cid", List.of(missing), Optional.empty(), Optional.empty()))
        .isInstanceOf(StatusRuntimeException.class);
  }

  private RelationPinSet resolve(QueryInput input) {
    return resolve(input, Optional.empty());
  }

  private RelationPinSet resolve(QueryInput input, Optional<Timestamp> asOfDefault) {
    return resolver
        .resolveInputs("cid", List.of(input), asOfDefault, Optional.empty())
        .relationPinSet();
  }

  private static NameRef name(String catalog, String table) {
    return NameRef.newBuilder().setCatalog(catalog).setName(table).build();
  }
}

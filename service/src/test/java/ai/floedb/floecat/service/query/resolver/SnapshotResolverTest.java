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

package ai.floedb.floecat.service.query.resolver;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.query.impl.QueryContext;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.junit.jupiter.api.Test;

class SnapshotResolverTest {

  private final SnapshotResolver resolver = new SnapshotResolver();

  private static ResourceId tableId(String id) {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setId(id)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }

  private static ResourceId viewId(String id) {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setId(id)
        .setKind(ResourceKind.RK_VIEW)
        .build();
  }

  @Test
  void snapshotIdOverrideWins() {
    var ctx = mock(QueryContext.class);
    var t = tableId("t1");

    var in =
        QueryInput.newBuilder()
            .setTableId(t)
            .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(123L).build())
            .build();

    var out = resolver.resolvePins("corr", ctx, List.of(in));
    assertEquals(1, out.size());
    assertEquals(t, out.get(0).getTableId());
    assertEquals(123L, out.get(0).getSnapshotId());

    verify(ctx, never()).requireSnapshotPin(any(), anyString());
  }

  @Test
  void asOfOverrideWins() {
    var ctx = mock(QueryContext.class);
    var t = tableId("t1");

    Timestamp ts = Timestamp.newBuilder().setSeconds(100).build();
    var in =
        QueryInput.newBuilder()
            .setTableId(t)
            .setSnapshot(SnapshotRef.newBuilder().setAsOf(ts).build())
            .build();

    var out = resolver.resolvePins("corr", ctx, List.of(in));
    assertEquals(1, out.size());
    assertEquals(t, out.get(0).getTableId());
    assertTrue(out.get(0).hasAsOf());
    assertEquals(ts, out.get(0).getAsOf());

    verify(ctx, never()).requireSnapshotPin(any(), anyString());
  }

  @Test
  void noOverrideUsesPinned() {
    var ctx = mock(QueryContext.class);
    var t = tableId("t1");

    var pinned = SnapshotPin.newBuilder().setTableId(t).setSnapshotId(7L).build();
    when(ctx.requireSnapshotPin(eq(t), anyString())).thenReturn(pinned);

    var in = QueryInput.newBuilder().setTableId(t).build();

    var out = resolver.resolvePins("corr", ctx, List.of(in));
    assertEquals(1, out.size());
    assertEquals(pinned, out.get(0));

    verify(ctx, times(1)).requireSnapshotPin(eq(t), eq("corr"));
  }

  @Test
  void whichNotSetUsesPinned() {
    var ctx = mock(QueryContext.class);
    var t = tableId("t1");

    var pinned = SnapshotPin.newBuilder().setTableId(t).setSnapshotId(7L).build();
    when(ctx.requireSnapshotPin(eq(t), anyString())).thenReturn(pinned);

    var in =
        QueryInput.newBuilder()
            .setTableId(t)
            // explicitly present but WHICH_NOT_SET
            .setSnapshot(SnapshotRef.newBuilder().build())
            .build();

    var out = resolver.resolvePins("corr", ctx, List.of(in));
    assertEquals(1, out.size());
    assertEquals(pinned, out.get(0));

    verify(ctx, times(1)).requireSnapshotPin(eq(t), eq("corr"));
  }

  @Test
  void specialCurrentUsesPinned() {
    var ctx = mock(QueryContext.class);
    var t = tableId("t1");

    var pinned = SnapshotPin.newBuilder().setTableId(t).setSnapshotId(7L).build();
    when(ctx.requireSnapshotPin(eq(t), anyString())).thenReturn(pinned);

    var in =
        QueryInput.newBuilder()
            .setTableId(t)
            .setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build())
            .build();

    var out = resolver.resolvePins("corr", ctx, List.of(in));
    assertEquals(1, out.size());
    assertEquals(pinned, out.get(0));

    verify(ctx, times(1)).requireSnapshotPin(eq(t), eq("corr"));
  }

  @Test
  void specialNonCurrentRejected() {
    var ctx = mock(QueryContext.class);
    var t = tableId("t1");

    // Pick any non-current enum value your proto defines.
    // If SS_UNSPECIFIED doesn't exist in your proto, replace with another non-current value.
    var in =
        QueryInput.newBuilder()
            .setTableId(t)
            .setSnapshot(
                SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_UNSPECIFIED).build())
            .build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class, () -> resolver.resolvePins("corr", ctx, List.of(in)));
    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());

    verify(ctx, never()).requireSnapshotPin(any(), anyString());
  }

  @Test
  void viewInputReturnsDummyPinAndDoesNotTouchContext() {
    var ctx = mock(QueryContext.class);
    var v = viewId("v1");

    var in = QueryInput.newBuilder().setViewId(v).build();

    var out = resolver.resolvePins("corr", ctx, List.of(in));
    assertEquals(1, out.size());

    // “dummy pin”: tableId == viewId to preserve output cardinality
    assertEquals(v, out.get(0).getTableId());

    verify(ctx, never()).requireSnapshotPin(any(), anyString());
  }

  @Test
  void neitherTableNorViewRejected() {
    var ctx = mock(QueryContext.class);

    var in = QueryInput.newBuilder().build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class, () -> resolver.resolvePins("corr", ctx, List.of(in)));
    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }
}

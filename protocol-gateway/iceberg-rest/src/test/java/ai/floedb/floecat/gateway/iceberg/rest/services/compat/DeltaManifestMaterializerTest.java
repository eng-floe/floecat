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

package ai.floedb.floecat.gateway.iceberg.rest.services.compat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.execution.rpc.ScanBundle;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.QueryClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.QuerySchemaClient;
import ai.floedb.floecat.query.rpc.BeginQueryResponse;
import ai.floedb.floecat.query.rpc.DescribeInputsResponse;
import ai.floedb.floecat.query.rpc.FetchScanBundleResponse;
import ai.floedb.floecat.query.rpc.QueryDescriptor;
import java.util.List;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeltaManifestMaterializerTest {

  private final QueryClient queryClient = mock(QueryClient.class);
  private final QuerySchemaClient querySchemaClient = mock(QuerySchemaClient.class);
  private final TestDeltaManifestMaterializer materializer = new TestDeltaManifestMaterializer();

  @BeforeEach
  void setUp() {
    materializer.queryClient = queryClient;
    materializer.querySchemaClient = querySchemaClient;

    when(queryClient.beginQuery(any()))
        .thenReturn(
            BeginQueryResponse.newBuilder()
                .setQuery(QueryDescriptor.newBuilder().setQueryId("q-1").build())
                .build());
    when(querySchemaClient.describeInputs(any()))
        .thenReturn(DescribeInputsResponse.newBuilder().build());

    ScanFile scanFile =
        ScanFile.newBuilder()
            .setFilePath("s3://floecat-delta/call_center/part-00000.parquet")
            .setFileFormat("PARQUET")
            .setFileSizeInBytes(256)
            .setRecordCount(42)
            .build();
    when(queryClient.fetchScanBundle(any()))
        .thenReturn(
            FetchScanBundleResponse.newBuilder()
                .setBundle(ScanBundle.newBuilder().addDataFiles(scanFile).build())
                .build());
  }

  @Test
  void materializeReusesCompatManifestListWhenSnapshotUnchanged() {
    Table table = deltaTable("reuse");
    Snapshot snapshot = snapshot(7L, 7L);

    List<Snapshot> first = materializer.materialize(table, List.of(snapshot));
    String firstManifestList = first.get(0).getManifestList();
    assertFalse(firstManifestList.isBlank());
    assertTrue(firstManifestList.endsWith("/metadata/snap-7-compat.avro"));

    List<Snapshot> second = materializer.materialize(table, List.of(snapshot));
    String secondManifestList = second.get(0).getManifestList();
    assertEquals(firstManifestList, secondManifestList);

    verify(queryClient, times(1)).fetchScanBundle(any());
  }

  @Test
  void materializeRegeneratesCompatManifestListWhenSnapshotChanges() {
    Table table = deltaTable("regenerate");
    Snapshot oldSnapshot = snapshot(7L, 7L);
    Snapshot newSnapshot = snapshot(8L, 8L);

    String oldManifestList =
        materializer.materialize(table, List.of(oldSnapshot)).get(0).getManifestList();
    String newManifestList =
        materializer.materialize(table, List.of(newSnapshot)).get(0).getManifestList();

    assertFalse(oldManifestList.isBlank());
    assertFalse(newManifestList.isBlank());
    assertNotEquals(oldManifestList, newManifestList);
    assertTrue(newManifestList.endsWith("/metadata/snap-8-compat.avro"));

    verify(queryClient, times(2)).fetchScanBundle(any());
  }

  private Table deltaTable(String testName) {
    return Table.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId("cat:examples:delta:call_center").build())
        .putProperties("storage_location", "s3://floecat-delta/call_center/" + testName)
        .build();
  }

  private Snapshot snapshot(long snapshotId, long sequence) {
    return Snapshot.newBuilder().setSnapshotId(snapshotId).setSequenceNumber(sequence).build();
  }

  private static final class TestDeltaManifestMaterializer extends DeltaManifestMaterializer {
    private final ReopenableInMemoryFileIO fileIo = new ReopenableInMemoryFileIO();

    @Override
    protected FileIO newFileIo(Table table) {
      return fileIo;
    }
  }

  private static final class ReopenableInMemoryFileIO extends InMemoryFileIO {
    @Override
    public void close() {
      // Keep files available across multiple materialize() calls in the same test.
    }
  }
}

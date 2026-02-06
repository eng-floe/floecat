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

package ai.floedb.floecat.gateway.iceberg.rest.services.client;

import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.BeginQueryResponse;
import ai.floedb.floecat.query.rpc.DataFile;
import ai.floedb.floecat.query.rpc.DataFileBatch;
import ai.floedb.floecat.query.rpc.DeleteFile;
import ai.floedb.floecat.query.rpc.DeleteFileBatch;
import ai.floedb.floecat.query.rpc.EndQueryRequest;
import ai.floedb.floecat.query.rpc.GetQueryRequest;
import ai.floedb.floecat.query.rpc.GetQueryResponse;
import ai.floedb.floecat.query.rpc.InitScanRequest;
import ai.floedb.floecat.query.rpc.InitScanResponse;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.query.rpc.ScanHandle;
import ai.floedb.floecat.query.rpc.TableInfo;
import com.google.common.collect.ImmutableList;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.jboss.logging.Logger;

@ApplicationScoped
public class QueryClient {
  private static final Logger LOG = Logger.getLogger(QueryClient.class);

  private final GrpcWithHeaders grpc;

  @Inject
  public QueryClient(GrpcWithHeaders grpc) {
    this.grpc = grpc;
  }

  public BeginQueryResponse beginQuery(BeginQueryRequest request) {
    return queryStub().beginQuery(request);
  }

  public GetQueryResponse getQuery(GetQueryRequest request) {
    return queryStub().getQuery(request);
  }

  public void endQuery(EndQueryRequest request) {
    queryStub().endQuery(request);
  }

  public ScanFileStreamResponse fetchScanFileStream(InitScanRequest request) {
    InitScanResponse resp = queryScanStub().initScan(request);
    TableInfo tableInfo = resp.getTableInfo();
    ScanHandle handle = resp.getHandle();

    List<DeleteFile> deleteFiles = new ArrayList<>();
    var deleteIterator = queryScanStub().streamDeleteFiles(handle);
    while (deleteIterator.hasNext()) {
      DeleteFileBatch batch = deleteIterator.next();
      for (var delete : batch.getItemsList()) {
        deleteFiles.add(delete);
      }
    }

    List<DataFile> dataFiles = new ArrayList<>();
    var dataIterator = queryScanStub().streamDataFiles(handle);
    while (dataIterator.hasNext()) {
      DataFileBatch batch = dataIterator.next();
      dataFiles.addAll(batch.getItemsList());
    }

    try {
      queryScanStub().closeScan(handle);
    } catch (Exception e) {
      LOG.debug("closeScan failed", e);
    }

    return new ScanFileStreamResponse(
        tableInfo, ImmutableList.copyOf(dataFiles), ImmutableList.copyOf(deleteFiles));
  }

  public record ScanFileStreamResponse(
      TableInfo tableInfo, List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {}

  public QueryServiceGrpc.QueryServiceBlockingStub queryStub() {
    return grpc.withHeaders(grpc.raw().query());
  }

  public QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScanStub() {
    return grpc.withHeaders(grpc.raw().queryScan());
  }
}

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

package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.statistics.rpc.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class StatsCaptureIT {
  @GrpcClient("floecat")
  StatsCaptureGrpc.StatsCaptureBlockingStub statscapture;

  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void listJobs() {
    var r = statscapture.listJobs(ListJobsRequest.newBuilder().build());
  }

  @Test
  void analyze() {
    var r = statscapture.analyze(AnalyzeRequest.newBuilder().build());
  }

  @Test
  void analyzeTableRejectsSeededTableWithoutUpstreamConnector() {
    ResourceId tableId =
        TestSupport.resolveTableId(directory, "examples", List.of("iceberg"), "orders");

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                statscapture.analyzeTable(
                    AnalyzeTableRequest.newBuilder().setTableId(tableId).build()));
    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }

  @Test
  void getJobs() {
    var r = statscapture.getJob(GetJobRequest.newBuilder().build());
  }

  @Test
  void cancelJobs() {
    var r = statscapture.cancelJob(CancelJobRequest.newBuilder().build());
  }
}

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

package ai.floedb.floecat.service.statistics.impl;

import ai.floedb.floecat.statistics.rpc.AnalyzeRequest;
import ai.floedb.floecat.statistics.rpc.AnalyzeResponse;
import ai.floedb.floecat.statistics.rpc.CancelJobRequest;
import ai.floedb.floecat.statistics.rpc.CancelJobResponse;
import ai.floedb.floecat.statistics.rpc.GetJobRequest;
import ai.floedb.floecat.statistics.rpc.GetJobResponse;
import ai.floedb.floecat.statistics.rpc.ListJobsRequest;
import ai.floedb.floecat.statistics.rpc.ListJobsResponse;
import ai.floedb.floecat.statistics.rpc.StatsCapture;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;

@GrpcService
public class StatsCaptureImpl implements StatsCapture {

  @Override
  public Uni<ListJobsResponse> listJobs(ListJobsRequest request) {
    return Uni.createFrom()
        .item(
            () -> {
              return ListJobsResponse.newBuilder().build();
            });
  }

  @Override
  public Uni<AnalyzeResponse> analyze(AnalyzeRequest request) {
    return Uni.createFrom()
        .item(
            () -> {
              return AnalyzeResponse.newBuilder().build();
            });
  }

  @Override
  public Uni<GetJobResponse> getJob(GetJobRequest request) {
    return Uni.createFrom()
        .item(
            () -> {
              return GetJobResponse.newBuilder().build();
            });
  }

  @Override
  public Uni<CancelJobResponse> cancelJob(CancelJobRequest request) {
    return Uni.createFrom()
        .item(
            () -> {
              return CancelJobResponse.newBuilder().build();
            });
  }
}

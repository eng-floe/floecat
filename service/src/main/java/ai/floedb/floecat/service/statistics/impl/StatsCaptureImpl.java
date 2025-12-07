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

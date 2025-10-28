package ai.floedb.metacat.service.statistic.impl;

import ai.floedb.metacat.statistic.rpc.AnalyzeRequest;
import ai.floedb.metacat.statistic.rpc.AnalyzeResponse;
import ai.floedb.metacat.statistic.rpc.CancelJobRequest;
import ai.floedb.metacat.statistic.rpc.CancelJobResponse;
import ai.floedb.metacat.statistic.rpc.GetJobRequest;
import ai.floedb.metacat.statistic.rpc.GetJobResponse;
import ai.floedb.metacat.statistic.rpc.ListJobsRequest;
import ai.floedb.metacat.statistic.rpc.ListJobsResponse;
import ai.floedb.metacat.statistic.rpc.StatsCapture;
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

package ai.floedb.metacat.service.statistic.impl;

import ai.floedb.metacat.statistic.rpc.*;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;

@GrpcService
public class StatsCaptureImpl implements StatsCapture {

    @Override
    public Uni<ListJobsResponse> listJobs(ListJobsRequest request) {
        return Uni.createFrom().item(() -> {
            return ListJobsResponse.newBuilder().build();
        });
    }

    @Override
    public Uni<AnalyzeResponse> analyze(AnalyzeRequest request) {
        return Uni.createFrom().item(() -> {
            return AnalyzeResponse.newBuilder().build();
        });
    }

    @Override
    public Uni<GetJobResponse> getJob(GetJobRequest request) {
        return Uni.createFrom().item(() -> {
            return GetJobResponse.newBuilder().build();
        });
    }

    @Override
    public Uni<CancelJobResponse> cancelJob(CancelJobRequest request) {
        return Uni.createFrom().item(() -> {
            return CancelJobResponse.newBuilder().build();
        });
    }

}

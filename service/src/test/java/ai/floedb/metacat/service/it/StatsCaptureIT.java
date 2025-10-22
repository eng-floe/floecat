package ai.floedb.metacat.service.it;

import ai.floedb.metacat.statistic.rpc.*;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class StatsCaptureIT {
  @GrpcClient("statscapture")
  StatsCaptureGrpc.StatsCaptureBlockingStub statscapture;

  @Test
  void listJobs() {
    var r = statscapture.listJobs(ListJobsRequest.newBuilder().build());
  }

  @Test
  void analyze() {
    var r = statscapture.analyze(AnalyzeRequest.newBuilder().build());
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

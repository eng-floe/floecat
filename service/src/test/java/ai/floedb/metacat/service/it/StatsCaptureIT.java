package ai.floedb.metacat.service.it;

import ai.floedb.metacat.service.bootstrap.impl.SeedRunner;
import ai.floedb.metacat.service.util.TestDataResetter;
import ai.floedb.metacat.statistics.rpc.*;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class StatsCaptureIT {
  @GrpcClient("metacat")
  StatsCaptureGrpc.StatsCaptureBlockingStub statscapture;

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
  void getJobs() {
    var r = statscapture.getJob(GetJobRequest.newBuilder().build());
  }

  @Test
  void cancelJobs() {
    var r = statscapture.cancelJob(CancelJobRequest.newBuilder().build());
  }
}

package ai.floedb.floecat.service.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.grpc.Context;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class GrpcContextUtilTest {

  @Test
  void preservesGrpcContextAcrossThreadHops() throws Exception {
    PrincipalContext expected =
        PrincipalContext.newBuilder()
            .setAccountId("acct")
            .setSubject("tester")
            .setCorrelationId("cid-123")
            .build();

    Context contextWithPrincipal = Context.current().withValue(PrincipalProvider.KEY, expected);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      CompletableFuture<PrincipalContext> observed = new CompletableFuture<>();
      contextWithPrincipal.run(
          () -> {
            GrpcContextUtil captured = GrpcContextUtil.capture();
            executor.submit(
                () -> captured.run(() -> observed.complete(PrincipalProvider.KEY.get())));
          });

      assertEquals(expected, observed.get(5, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }
  }
}

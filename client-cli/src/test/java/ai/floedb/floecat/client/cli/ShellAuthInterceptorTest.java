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

package ai.floedb.floecat.client.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.account.rpc.AccountServiceGrpc;
import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableConstraintsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableIndexServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import ai.floedb.floecat.storage.rpc.ListStorageAuthoritiesRequest;
import ai.floedb.floecat.storage.rpc.ListStorageAuthoritiesResponse;
import ai.floedb.floecat.storage.rpc.StorageAuthoritiesGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ShellAuthInterceptorTest {

  private static final Metadata.Key<String> SESSION_HEADER =
      Metadata.Key.of("x-floe-session", Metadata.ASCII_STRING_MARSHALLER);

  @Test
  void applyAuthInterceptorsAlsoWrapsStorageAuthoritiesStub() throws Exception {
    AtomicReference<String> observedSession = new AtomicReference<>();
    String serverName = InProcessServerBuilder.generateName();
    Server server =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .intercept(new CaptureSessionHeaderInterceptor(observedSession))
            .addService(new StorageAuthorityService())
            .build()
            .start();
    ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    try {
      Shell shell = new Shell();
      shell.sessionToken = "session-123";
      shell.authHeaderName = "authorization";
      shell.sessionHeaderName = "x-floe-session";
      shell.accountHeaderName = "x-floe-account";
      shell.catalogs = CatalogServiceGrpc.newBlockingStub(channel);
      shell.namespaces = NamespaceServiceGrpc.newBlockingStub(channel);
      shell.tables = TableServiceGrpc.newBlockingStub(channel);
      shell.directory = DirectoryServiceGrpc.newBlockingStub(channel);
      shell.statistics = TableStatisticsServiceGrpc.newBlockingStub(channel);
      shell.indexes = TableIndexServiceGrpc.newBlockingStub(channel);
      shell.constraintsService = TableConstraintsServiceGrpc.newBlockingStub(channel);
      shell.snapshots = SnapshotServiceGrpc.newBlockingStub(channel);
      shell.viewService = ViewServiceGrpc.newBlockingStub(channel);
      shell.connectors = ConnectorsGrpc.newBlockingStub(channel);
      shell.reconcileControl = ReconcileControlGrpc.newBlockingStub(channel);
      shell.queries = QueryServiceGrpc.newBlockingStub(channel);
      shell.queryScan = QueryScanServiceGrpc.newBlockingStub(channel);
      shell.querySchema = QuerySchemaServiceGrpc.newBlockingStub(channel);
      shell.accounts = AccountServiceGrpc.newBlockingStub(channel);
      shell.storageAuthorities = StorageAuthoritiesGrpc.newBlockingStub(channel);

      Method applyAuthInterceptors = Shell.class.getDeclaredMethod("applyAuthInterceptors");
      applyAuthInterceptors.setAccessible(true);
      applyAuthInterceptors.invoke(shell);

      shell.storageAuthorities.listStorageAuthorities(
          ListStorageAuthoritiesRequest.getDefaultInstance());

      assertEquals("session-123", observedSession.get());
    } finally {
      channel.shutdownNow();
      server.shutdownNow();
    }
  }

  private static final class CaptureSessionHeaderInterceptor implements ServerInterceptor {
    private final AtomicReference<String> observedSession;

    private CaptureSessionHeaderInterceptor(AtomicReference<String> observedSession) {
      this.observedSession = observedSession;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
      observedSession.set(headers.get(SESSION_HEADER));
      return next.startCall(call, headers);
    }
  }

  private static final class StorageAuthorityService
      extends StorageAuthoritiesGrpc.StorageAuthoritiesImplBase {
    @Override
    public void listStorageAuthorities(
        ListStorageAuthoritiesRequest request,
        StreamObserver<ListStorageAuthoritiesResponse> responseObserver) {
      responseObserver.onNext(ListStorageAuthoritiesResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }
}

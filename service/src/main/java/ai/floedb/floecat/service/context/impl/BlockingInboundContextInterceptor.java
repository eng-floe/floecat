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

package ai.floedb.floecat.service.context.impl;

import ai.floedb.floecat.service.repo.impl.AccountRepository;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.quarkus.grpc.GlobalInterceptor;
import io.quarkus.oidc.TenantIdentityProvider;
import io.vertx.core.Vertx;
import io.vertx.grpc.BlockingServerInterceptor;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
@GlobalInterceptor
@Priority(0) // Must run before other interceptors
public class BlockingInboundContextInterceptor implements ServerInterceptor {
  private static final Logger LOG = Logger.getLogger(BlockingInboundContextInterceptor.class);

  private final ServerInterceptor delegate;

  @Inject
  public BlockingInboundContextInterceptor(
      Vertx vertx,
      AccountRepository accountRepository,
      TenantIdentityProvider identityProvider,
      @ConfigProperty(name = "floecat.interceptor.validate.account", defaultValue = "true")
          boolean validateAccount,
      @ConfigProperty(name = "floecat.interceptor.session.header") Optional<String> sessionHeader,
      @ConfigProperty(name = "floecat.interceptor.authorization.header")
          Optional<String> authorizationHeader,
      @ConfigProperty(
              name = "floecat.interceptor.dev-account.header",
              defaultValue = "x-floe-account")
          Optional<String> devAccountHeader,
      @ConfigProperty(name = "floecat.auth.mode", defaultValue = "oidc") String authMode,
      @ConfigProperty(
              name = "floecat.interceptor.session.account-claim",
              defaultValue = "account_id")
          String accountClaimName,
      @ConfigProperty(name = "floecat.interceptor.session.role-claim", defaultValue = "roles")
          String roleClaimName,
      // Escape hatch: set to true (env: FLOECAT_INTERCEPTOR_BLOCKING_LEGACY_WRAP=true) to
      // revert to the deprecated io.vertx.grpc.BlockingServerInterceptor.wrap. Intended for
      // before/after validation of the context-preserving fix; leave unset in production.
      @ConfigProperty(name = "floecat.interceptor.blocking.legacy-wrap", defaultValue = "false")
          boolean legacyWrap) {

    // Plain helper with your logic; does NOT implement ServerInterceptor (avoids Quarkus "unused"
    // warnings).
    final InboundContextInterceptor inbound =
        new InboundContextInterceptor(
            accountRepository,
            identityProvider,
            validateAccount,
            sessionHeader,
            authorizationHeader,
            devAccountHeader,
            authMode,
            accountClaimName,
            roleClaimName);

    // Wrap so the inner interceptor (and the rest of the chain it builds) runs on a Vert.x
    // worker thread, off the event-loop. Replaces io.vertx.grpc.BlockingServerInterceptor.wrap,
    // which is deprecated and does not propagate io.grpc.Context across the buffered-event
    // replay — that gap, combined with Quarkus's GrpcDuplicatedContextGrpcInterceptor hopping
    // via runOnContext on Vert.x-context mismatch, drops the principal/correlation context on a
    // subset of inbound calls. The replacement below is modelled on Quarkus's internal
    // BlockingServerInterceptor + BlockingExecutionHandler pattern (which is only active for
    // @Blocking-annotated methods); here it applies to every method because the gRPC services
    // in this module are uniformly blocking.
    //
    // ServerInterceptor is a functional interface, so we pass the helper's interceptCall as a
    // method reference — no JDK Proxy or runtime classloader gymnastics required.
    if (legacyWrap) {
      LOG.warnf(
          "Using deprecated io.vertx.grpc.BlockingServerInterceptor.wrap"
              + " (floecat.interceptor.blocking.legacy-wrap=true)."
              + " This reintroduces the gRPC Context drop documented in the fix's commit;"
              + " expect intermittent empty PrincipalContext on inbound calls.");
      this.delegate = BlockingServerInterceptor.wrap(vertx, inbound::interceptCall);
    } else {
      this.delegate = new CtxPropagatingBlockingWrap(vertx, inbound::interceptCall);
    }
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    return delegate.interceptCall(call, headers, next);
  }
}

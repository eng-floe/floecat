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

import ai.floedb.floecat.service.common.GrpcInterceptorPriorities;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.quarkus.grpc.GlobalInterceptor;
import io.quarkus.oidc.TenantIdentityProvider;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.Prioritized;
import jakarta.inject.Inject;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
@GlobalInterceptor
public class BlockingInboundContextInterceptor implements ServerInterceptor, Prioritized {
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
          String roleClaimName) {

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

    // ServerInterceptor is a functional interface, so we pass the helper's interceptCall as a
    // method reference — no JDK Proxy or runtime classloader gymnastics required.
    this.delegate = new CtxPropagatingBlockingWrap(vertx, inbound::interceptCall)::interceptCall;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    return delegate.interceptCall(call, headers, next);
  }

  /**
   * Highest of floecat's interceptors so this one runs outermost among them (higher = outer; see
   * {@link GrpcInterceptorPriorities}): the call context and MDC must be populated before
   * telemetry/logging read them. Only the {@code Prioritized} interface is honored by Quarkus's
   * interceptor comparator — a {@code @Priority} annotation is silently ignored.
   */
  @Override
  public int getPriority() {
    return GrpcInterceptorPriorities.INBOUND_CONTEXT;
  }
}

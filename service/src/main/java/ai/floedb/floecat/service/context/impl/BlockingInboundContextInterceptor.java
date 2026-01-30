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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
@GlobalInterceptor
@Priority(0) // Must run before other interceptors
public class BlockingInboundContextInterceptor implements ServerInterceptor {
  private final ServerInterceptor delegate;

  @Inject
  public BlockingInboundContextInterceptor(
      Vertx vertx,
      AccountRepository accountRepository,
      TenantIdentityProvider identityProvider,
      @ConfigProperty(name = "floecat.interceptor.validate.account", defaultValue = "true")
          boolean validateAccount,
      @ConfigProperty(name = "floecat.interceptor.session.header") Optional<String> sessionHeader,
      @ConfigProperty(name = "floecat.interceptor.allow.dev-context", defaultValue = "true")
          boolean allowDevContext,
      @ConfigProperty(
              name = "floecat.interceptor.session.account-claim",
              defaultValue = "account_id")
          String accountClaimName,
      @ConfigProperty(name = "floecat.interceptor.session.role-claim", defaultValue = "roles")
          String roleClaimName,
      @ConfigProperty(name = "floecat.interceptor.allow.principal-header", defaultValue = "false")
          boolean allowPrincipalHeader) {

    // Plain helper with your logic; does NOT implement ServerInterceptor (avoids Quarkus "unused"
    // warnings).
    final InboundContextInterceptor inbound =
        new InboundContextInterceptor(
            accountRepository,
            identityProvider,
            validateAccount,
            sessionHeader,
            allowDevContext,
            accountClaimName,
            roleClaimName,
            allowPrincipalHeader);

    // Create a ServerInterceptor at runtime that delegates to your helper.
    ServerInterceptor runtimeInterceptor =
        (ServerInterceptor)
            Proxy.newProxyInstance(
                ServerInterceptor.class.getClassLoader(),
                new Class<?>[] {ServerInterceptor.class},
                new InvocationHandler() {
                  @Override
                  @SuppressWarnings({"unchecked", "rawtypes"})
                  public Object invoke(Object proxy, Method method, Object[] args)
                      throws Throwable {
                    // Handle Object methods cleanly
                    switch (method.getName()) {
                      case "toString":
                        return "InboundContextInterceptorProxy";
                      case "hashCode":
                        return System.identityHashCode(proxy);
                      case "equals":
                        return proxy == args[0];
                      default:
                        break;
                    }

                    if ("interceptCall".equals(method.getName())
                        && args != null
                        && args.length == 3) {
                      return inbound.interceptCall(
                          (ServerCall) args[0], (Metadata) args[1], (ServerCallHandler) args[2]);
                    }

                    throw new UnsupportedOperationException(
                        "Unexpected method on ServerInterceptor proxy: " + method);
                  }
                });

    // Vert.x pattern: wrap so it runs on a worker thread (off the event-loop).
    // [oai_citation:2‡GitHub](https://github.com/vert-x3/vertx-grpc/pull/22/changes)
    this.delegate = BlockingServerInterceptor.wrap(vertx, runtimeInterceptor);
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    return delegate.interceptCall(call, headers, next);
  }
}

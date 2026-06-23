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

package ai.floedb.floecat.service.security.impl;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import io.grpc.Context;
import io.grpc.Status;
import io.smallrye.common.vertx.VertxContext;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Resolves the principal for the current call. The principal is carried on two channels:
 *
 * <ul>
 *   <li>an {@link io.grpc.Context} key ({@link #KEY}), read by gRPC server interceptors that run
 *       <i>before</i> the call is dispatched to the service method; and
 *   <li>a Vert.x duplicated-context local ({@link #PRINCIPAL_LOCAL}), read by the service-method
 *       body, which runs <i>after</i> Quarkus's gRPC dispatch has hopped the call onto a Vert.x
 *       worker.
 * </ul>
 *
 * <p>The second channel exists because {@link io.grpc.Context} is not reliable across that worker
 * thread-hop: {@code io.grpc.override.ContextStorageOverride} stores the context as a
 * duplicated-context local only while it is attached on a duplicated context, and otherwise falls
 * back to a plain {@link ThreadLocal}. When the principal is attached on one context and read on a
 * different worker without a re-attach, the thread-local fallback diverges and {@link #KEY} reads
 * back empty — surfacing as a misleading {@code PERMISSION_DENIED}. A duplicated-context local, on
 * the other hand, is keyed off the (per-call) duplicated context itself, so it survives the hop for
 * exactly the same reason MDC does, and cannot bleed between concurrent calls.
 */
@ApplicationScoped
public class PrincipalProvider {
  public static final Context.Key<PrincipalContext> KEY = Context.key("principal");

  /** Vert.x duplicated-context local key under which the resolved principal is mirrored. */
  private static final String PRINCIPAL_LOCAL = "floecat.principal";

  @Inject AccountRepository accountRepository;

  @ConfigProperty(name = "floecat.interceptor.validate.account", defaultValue = "true")
  boolean validateAccount;

  public PrincipalProvider() {}

  PrincipalProvider(AccountRepository accountRepository, boolean validateAccount) {
    this.accountRepository = accountRepository;
    this.validateAccount = validateAccount;
  }

  /**
   * Returns the principal for the current call.
   *
   * <p>The duplicated-context local is preferred over {@link #KEY}: every read through this method
   * happens in the service-method body (the interceptors read {@link #KEY} directly), where the
   * local is the per-call value that survived the worker hop, while {@link #KEY} may be empty or —
   * via its thread-local fallback on a reused worker — carry a stale value. Falls back to {@link
   * #KEY} for callers that resolve the principal without a Vert.x duplicated context (e.g. unit
   * tests that drive {@link io.grpc.Context} directly), and to the empty default when neither
   * channel carries a principal.
   */
  public PrincipalContext get() {
    PrincipalContext fromContextLocal = fromDuplicatedContext();
    if (fromContextLocal != null) {
      return requireKnownAccount(fromContextLocal);
    }
    PrincipalContext fromGrpc = KEY.get();
    if (fromGrpc != null) {
      return requireKnownAccount(fromGrpc);
    }
    return PrincipalContext.getDefaultInstance();
  }

  /**
   * Mirrors the resolved principal onto the current Vert.x duplicated context so it survives the
   * gRPC dispatch worker thread-hop into the service-method body. Called from the inbound
   * interceptor alongside MDC population, on the same context MDC is written to.
   *
   * <p>No-op when the current call is not on a Vert.x duplicated context (the {@link
   * io.grpc.Context} key remains the carrier in that case), so it is safe to call unconditionally.
   */
  public static void storeOnDuplicatedContext(PrincipalContext principalContext) {
    io.vertx.core.Context ctx = Vertx.currentContext();
    if (ctx != null && VertxContext.isDuplicatedContext(ctx)) {
      ctx.putLocal(PRINCIPAL_LOCAL, principalContext);
    }
  }

  private static PrincipalContext fromDuplicatedContext() {
    io.vertx.core.Context ctx = Vertx.currentContext();
    if (ctx != null && VertxContext.isDuplicatedContext(ctx)) {
      Object value = ctx.getLocal(PRINCIPAL_LOCAL);
      if (value instanceof PrincipalContext principalContext) {
        return principalContext;
      }
    }
    return null;
  }

  private PrincipalContext requireKnownAccount(PrincipalContext principalContext) {
    if (!validateAccount || principalContext == null) {
      return principalContext;
    }
    String accountId = principalContext.getAccountId();
    if (accountId == null || accountId.isBlank() || accountRepository == null) {
      return principalContext;
    }
    ResourceId accountRid =
        ResourceId.newBuilder().setId(accountId).setKind(ResourceKind.RK_ACCOUNT).build();
    if (accountRepository.getById(accountRid).isEmpty()) {
      throw Status.UNAUTHENTICATED
          .withDescription("invalid or unknown account: " + accountId)
          .asRuntimeException();
    }
    return principalContext;
  }
}

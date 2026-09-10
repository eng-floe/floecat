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

package ai.floedb.floecat.service.account;

import io.quarkus.runtime.ShutdownEvent;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.time.Duration;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Internal lifecycle endpoint used by the deployment controller before removing a Floecat pod.
 *
 * <p>{@code POST /internal/drain} closes local admission and returns {@code 202} until all local
 * account work and roots have retired. The controller must keep the pod alive and poll the same
 * endpoint until it receives {@code 200} with {@code drained=true}; this endpoint never writes KV
 * and is not part of the catalog data plane.
 */
@ApplicationScoped
public class FloecatDrainEndpoint {
  private static final String PATH = "/internal/drain";
  private static final long POLL_MILLIS = 25L;

  @Inject AccountGcAuthority authority;

  @ConfigProperty(name = "floecat.account-ownership.drain-timeout-ms", defaultValue = "180000")
  long defaultTimeoutMs;

  void routes(@Observes Router router) {
    router.post(PATH).handler(this::startDrain);
    router.get(PATH).handler(this::getDrain);
  }

  /**
   * Keep the safety fence effective even when a kubelet cannot reach the HTTP hook (for example
   * while the pod network policy is being torn down). Kubernetes still gives this observer the
   * pod's termination grace period, so SIGTERM is a local fallback for the same drain contract.
   */
  void onShutdown(@Observes ShutdownEvent ignored) {
    authority.beginProcessDrain();
    awaitDrained(Math.max(0L, defaultTimeoutMs));
  }

  /**
   * Kubernetes lifecycle hooks can issue only an HTTP GET. A plain GET remains a cheap status
   * probe, while {@code ?wait=true} uses the same drain path as the explicit POST contract.
   */
  private void getDrain(RoutingContext context) {
    String wait = context.request().getParam("wait");
    if (wait == null || wait.isBlank()) {
      status(context);
      return;
    }
    startDrain(context);
  }

  private void startDrain(RoutingContext context) {
    authority.beginProcessDrain();
    boolean wait = booleanParam(context, "wait", true);
    if (!wait) {
      respond(context, authority.processStatus());
      return;
    }
    long timeoutMs = timeoutMs(context);
    context
        .vertx()
        .<AccountGcAuthority.ProcessStatus>executeBlocking(
            promise -> promise.complete(awaitDrained(timeoutMs)), false)
        .onSuccess(result -> respond(context, result))
        .onFailure(context::fail);
  }

  private void status(RoutingContext context) {
    respond(context, authority.processStatus());
  }

  private AccountGcAuthority.ProcessStatus awaitDrained(long timeoutMs) {
    long deadline = System.nanoTime() + Duration.ofMillis(timeoutMs).toNanos();
    AccountGcAuthority.ProcessStatus status = authority.processStatus();
    while (!status.drained() && System.nanoTime() < deadline) {
      try {
        Thread.sleep(POLL_MILLIS);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return authority.processStatus();
      }
      status = authority.processStatus();
    }
    return status;
  }

  private void respond(RoutingContext context, AccountGcAuthority.ProcessStatus status) {
    int code = status.drained() ? 200 : 202;
    JsonObject body =
        new JsonObject()
            .put("processIncarnation", status.processIncarnation())
            .put("draining", status.draining())
            .put("drained", status.drained())
            .put("accounts", status.accounts())
            .put("servingAccounts", status.servingAccounts())
            .put("drainingAccounts", status.drainingAccounts())
            .put("activeResolutions", status.activeResolutions())
            .put("activeMutations", status.activeMutations())
            .put("activeGc", status.activeGc())
            .put("referencedRoots", status.referencedRoots());
    context
        .response()
        .setStatusCode(code)
        .putHeader("content-type", "application/json")
        .end(body.encode());
  }

  private long timeoutMs(RoutingContext context) {
    String value = context.request().getParam("timeoutMs");
    if (value == null || value.isBlank()) {
      return Math.max(0L, defaultTimeoutMs);
    }
    try {
      return Math.max(0L, Long.parseLong(value));
    } catch (NumberFormatException ignored) {
      return Math.max(0L, defaultTimeoutMs);
    }
  }

  private static boolean booleanParam(RoutingContext context, String name, boolean defaultValue) {
    String value = context.request().getParam(name);
    return value == null || value.isBlank() ? defaultValue : Boolean.parseBoolean(value);
  }
}

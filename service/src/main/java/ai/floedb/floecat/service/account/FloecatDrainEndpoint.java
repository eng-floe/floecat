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
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Pod lifecycle endpoint, registered only in managed mode and never writing KV.
 *
 * <p>{@code GET /internal/drain} reports process status. {@code GET|POST /internal/drain?wait=true}
 * moves the process to draining and returns {@code 200} once active mutations and resolutions are
 * zero, or {@code 202} at the timeout. Draining is irreversible for the life of the process, and
 * the endpoint authenticates no one: the {@code preStop} hook is a kubelet {@code httpGet} from the
 * node address, which no in-process check can tell from any other caller, so access is the mesh
 * authorization policy's job. The shutdown observer applies the same drain when the hook never
 * arrived.
 */
@ApplicationScoped
public class FloecatDrainEndpoint {
  private static final Logger LOG = Logger.getLogger(FloecatDrainEndpoint.class);
  static final String PATH = "/internal/drain";
  static final long MAX_TIMEOUT_MS = Duration.ofHours(1).toMillis();
  private static final long POLL_MILLIS = 25L;

  record Request(String method, boolean awaitDrain, String timeoutMsParam) {}

  record Response(int status, String body) {}

  @Inject AccountAssignment assignment;

  @Inject
  @ConfigProperty(name = "floecat.account-assignment.drain-timeout-ms", defaultValue = "110000")
  long defaultTimeoutMs;

  void routes(@Observes Router router) {
    if (!assignment.managed()) {
      return;
    }
    router.get(PATH).handler(this::handleHttp);
    router.post(PATH).handler(this::handleHttp);
  }

  /** SIGTERM fallback for a preStop hook that never reached the pod. */
  void onShutdown(@Observes ShutdownEvent ignored) {
    if (!assignment.managed()) {
      return;
    }
    assignment.beginProcessDrain();
    awaitDrained(clampTimeout(defaultTimeoutMs));
  }

  private void handleHttp(RoutingContext context) {
    Request request =
        new Request(
            context.request().method().name(),
            Boolean.parseBoolean(context.request().getParam("wait")),
            context.request().getParam("timeoutMs"));
    if (!request.awaitDrain()) {
      respond(context, handle(request));
      return;
    }
    context
        .vertx()
        .executeBlocking(() -> handle(request), false)
        .onSuccess(response -> respond(context, response))
        .onFailure(context::fail);
  }

  /** Transport-free request handling; the HTTP route and the tests both go through here. */
  Response handle(Request request) {
    if (!request.awaitDrain()) {
      if ("POST".equalsIgnoreCase(request.method())) {
        return respond(assignment.beginProcessDrain());
      }
      return respond(assignment.status());
    }
    Long timeoutMs = timeoutMs(request.timeoutMsParam());
    if (timeoutMs == null) {
      return new Response(400, error("timeoutMs must be a non-negative integer"));
    }
    assignment.beginProcessDrain();
    return respond(awaitDrained(timeoutMs));
  }

  /** Parsed and clamped to {@code [0, MAX_TIMEOUT_MS]}; null when the parameter is malformed. */
  Long timeoutMs(String parameter) {
    if (parameter == null || parameter.isBlank()) {
      return clampTimeout(defaultTimeoutMs);
    }
    try {
      long parsed = Long.parseLong(parameter.trim());
      return parsed < 0 ? null : clampTimeout(parsed);
    } catch (NumberFormatException malformed) {
      return null;
    }
  }

  static long clampTimeout(long timeoutMs) {
    return Math.max(0L, Math.min(MAX_TIMEOUT_MS, timeoutMs));
  }

  private AccountAssignment.Status awaitDrained(long timeoutMs) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    AccountAssignment.Status status = assignment.status();
    while (!status.drained() && System.nanoTime() - deadline < 0) {
      try {
        Thread.sleep(POLL_MILLIS);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return assignment.status();
      }
      status = assignment.status();
    }
    if (!status.drained()) {
      LOG.warnf(
          "account_assignment_drain_timeout member=%s active_resolutions=%d active_mutations=%d",
          status.memberId(), status.activeResolutions(), status.activeMutations());
    }
    return status;
  }

  private static Response respond(AccountAssignment.Status status) {
    long serving = 0;
    long draining = 0;
    for (AccountAssignment.AccountStatus account : status.accounts()) {
      if (account.mode() == AccountAssignment.AccountMode.SERVING) {
        serving++;
      } else if (account.mode() == AccountAssignment.AccountMode.DRAINING) {
        draining++;
      }
    }
    JsonObject body =
        new JsonObject()
            .put("memberId", status.memberId())
            .put("incarnation", status.incarnation())
            .put("epoch", status.epoch())
            .put("draining", status.processDraining())
            .put("drained", status.drained())
            .put("accounts", status.accounts().size())
            .put("servingAccounts", serving)
            .put("drainingAccounts", draining)
            .put("activeResolutions", status.activeResolutions())
            .put("activeMutations", status.activeMutations())
            .put("activeGc", status.activeGc());
    return new Response(status.drained() ? 200 : 202, body.encode());
  }

  private static String error(String message) {
    return new JsonObject().put("error", message).encode();
  }

  private static void respond(RoutingContext context, Response response) {
    context
        .response()
        .setStatusCode(response.status())
        .putHeader("content-type", "application/json")
        .end(response.body());
  }
}

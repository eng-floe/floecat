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
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Deployment-neutral pod lifecycle endpoint. It never writes KV.
 *
 * <p>{@code GET /internal/drain} reports process status. A {@code POST}, or any request carrying
 * {@code wait=true}, starts the drain -- except that {@code wait=true} validates {@code timeoutMs}
 * first and refuses a malformed or negative one with {@code 400}, draining nothing. Which requests
 * drain is pinned by {@code FloecatDrainEndpointTest.onlyTheseRequestsStartTheDrain}; the rule is
 * stated once in {@code docs/service.md}. Draining is irreversible for the life of the process, and
 * the endpoint authenticates no one: the {@code preStop} hook is a kubelet {@code httpGet} from the
 * node address, which no in-process check can tell from any other caller, so access is the mesh
 * authorization policy's job. The shutdown observer applies the same drain when the hook never
 * arrived.
 */
@ApplicationScoped
public class FloecatDrainEndpoint {
  private static final Logger LOG = Logger.getLogger(FloecatDrainEndpoint.class);
  static final String PATH = "/internal/drain";
  private static final long POLL_MILLIS = 25L;

  record Request(String method, boolean awaitDrain, String timeoutMsParam) {}

  record Response(int status, String body) {}

  @Inject LifecycleDrain drain;

  @Inject
  @ConfigProperty(name = "floecat.lifecycle-drain.timeout-ms", defaultValue = "110000")
  long defaultTimeoutMs;

  void routes(@Observes Router router) {
    router.get(PATH).handler(this::handleHttp);
    router.post(PATH).handler(this::handleHttp);
  }

  /** SIGTERM fallback for a preStop hook that never reached the pod. */
  void onShutdown(@Observes ShutdownEvent ignored) {
    drain.beginProcessDrain();
    awaitDrained(clampTimeout(defaultTimeoutMs));
  }

  private void handleHttp(RoutingContext context) {
    Request request =
        request(
            context.request().method().name(),
            context.request().getParam("wait"),
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

  /**
   * Reads {@code wait} the same way for the route and for tests: a boolean, so only {@code true}.
   */
  static Request request(String method, String waitParam, String timeoutMsParam) {
    return new Request(method, Boolean.parseBoolean(waitParam), timeoutMsParam);
  }

  /** Transport-free request handling; the HTTP route and the tests both go through here. */
  Response handle(Request request) {
    if (!request.awaitDrain()) {
      if ("POST".equalsIgnoreCase(request.method())) {
        return respond(drain.beginProcessDrain());
      }
      return respond(drain.status());
    }
    Long timeoutMs = timeoutMs(request.timeoutMsParam());
    if (timeoutMs == null) {
      return new Response(400, error("timeoutMs must be a non-negative integer"));
    }
    drain.beginProcessDrain();
    return respond(awaitDrained(timeoutMs));
  }

  /** Parsed as a non-negative duration; null when the parameter is malformed. */
  Long timeoutMs(String parameter) {
    if (parameter == null || parameter.isBlank()) {
      return clampTimeout(defaultTimeoutMs);
    }
    try {
      long parsed = Long.parseLong(parameter.trim());
      return parsed < 0 ? null : parsed;
    } catch (NumberFormatException malformed) {
      return null;
    }
  }

  static long clampTimeout(long timeoutMs) {
    return Math.max(0L, timeoutMs);
  }

  private LifecycleControl.Status awaitDrained(long timeoutMs) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    LifecycleControl.Status status = drain.status();
    while (!status.drained() && System.nanoTime() - deadline < 0) {
      try {
        Thread.sleep(POLL_MILLIS);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return drain.status();
      }
      status = drain.status();
    }
    if (!status.drained()) {
      LOG.warnf(
          "account_assignment_drain_timeout member=%s active_resolutions=%d active_mutations=%d",
          status.memberId(), status.activeResolutions(), status.activeMutations());
    }
    return status;
  }

  private static Response respond(LifecycleControl.Status status) {
    long serving = 0;
    long draining = 0;
    for (LifecycleControl.AccountStatus account : status.accounts()) {
      if (account.mode() == LifecycleControl.AccountMode.SERVING) {
        serving++;
      } else if (account.mode() == LifecycleControl.AccountMode.DRAINING) {
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
            .put("activeRpcs", status.activeRpcs())
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

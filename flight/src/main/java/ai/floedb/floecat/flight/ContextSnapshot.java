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

package ai.floedb.floecat.flight;

import io.grpc.Context;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.jboss.logging.MDC;

/** Snapshot of contextual helpers (MDC, gRPC Context, etc.) for worker threads. */
public final class ContextSnapshot {

  private final Map<String, Object> mdc;
  private final Context context;

  private ContextSnapshot(Map<String, Object> mdc, Context context) {
    this.mdc = mdc;
    this.context = context;
  }

  public static ContextSnapshot capture() {
    Map<String, Object> current = MDC.getMap();
    return new ContextSnapshot(copy(current), Context.current());
  }

  public AutoCloseable apply() {
    Map<String, Object> previous = MDC.getMap();
    if (mdc.isEmpty()) {
      MDC.clear();
    } else {
      setContext(mdc);
    }
    Context previousContext = context.attach();
    return () -> {
      context.detach(previousContext);
      if (previous == null || previous.isEmpty()) {
        MDC.clear();
      } else {
        setContext(previous);
      }
    };
  }

  private static Map<String, Object> copy(Map<String, Object> source) {
    if (source == null || source.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new HashMap<>(source));
  }

  private static void setContext(Map<String, Object> context) {
    MDC.clear();
    if (context == null || context.isEmpty()) {
      return;
    }
    context.forEach((k, v) -> MDC.put(k, v == null ? "" : v.toString()));
  }
}

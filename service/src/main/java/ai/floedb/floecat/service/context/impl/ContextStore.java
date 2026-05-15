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

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.security.impl.PrincipalContexts;
import io.grpc.Context;
import io.vertx.core.Vertx;
import java.util.HashMap;
import java.util.Map;
import org.jboss.logging.Logger;

/** Stores request-scoped gRPC context values in both gRPC and Vert.x local context. */
public final class ContextStore {
  private static final Logger LOG = Logger.getLogger(ContextStore.class);
  private static final Object VERTX_LOCAL_VALUES = new Object();
  private Context context;

  private ContextStore(Context base) {
    this.context = base == null ? Context.current() : base;
  }

  public static <T> Context set(Context base, Context.Key<T> key, T value) {
    return withValue(base, key, value).context();
  }

  public static <T> ContextStore withValue(Context base, Context.Key<T> key, T value) {
    return new ContextStore(base).withValue(key, value);
  }

  public static <T> T get(Context.Key<T> key) {
    T grpcValue = get(Context.current(), key);
    if (isUsable(grpcValue)) {
      return grpcValue;
    }

    T vertxValue = getVertxLocal(key);
    if (isUsable(vertxValue)) {
      return vertxValue;
    }

    return grpcValue != null ? grpcValue : vertxValue;
  }

  public static <T> T get(Context context, Context.Key<T> key) {
    return context == null ? null : key.get(context);
  }

  public static void clear() {
    var context = Vertx.currentContext();
    if (context == null) {
      return;
    }
    try {
      context.removeLocal(VERTX_LOCAL_VALUES);
    } catch (RuntimeException e) {
      LOG.warn("Failed to clear request context from Vert.x local context", e);
    }
  }

  private static boolean isUsable(Object value) {
    if (value == null) {
      return false;
    }
    if (value instanceof String text) {
      return !text.isBlank();
    }
    if (value instanceof PrincipalContext principalContext) {
      return !PrincipalContexts.isEmpty(principalContext);
    }
    if (value instanceof EngineContext engineContext) {
      return !engineContext.isEmpty();
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  private static <T> T getVertxLocal(Context.Key<T> key) {
    var context = Vertx.currentContext();
    if (context == null) {
      return null;
    }
    Object value = context.getLocal(VERTX_LOCAL_VALUES);
    if (!(value instanceof Map<?, ?> values)) {
      return null;
    }
    return (T) values.get(key);
  }

  private static <T> void setVertxLocal(Context.Key<T> key, T value) {
    var context = Vertx.currentContext();
    if (context == null) {
      return;
    }
    try {
      Object current = context.getLocal(VERTX_LOCAL_VALUES);
      Map<Context.Key<?>, Object> values;
      if (current instanceof Map<?, ?> existing) {
        values = new HashMap<>();
        existing.forEach(
            (existingKey, existingValue) ->
                values.put((Context.Key<?>) existingKey, existingValue));
      } else {
        values = new HashMap<>();
      }
      values.put(key, value);
      context.putLocal(VERTX_LOCAL_VALUES, values);
    } catch (RuntimeException e) {
      LOG.warnf(e, "Failed to store request context value for key %s in Vert.x local context", key);
    }
  }

  public <T> ContextStore withValue(Context.Key<T> key, T value) {
    setVertxLocal(key, value);
    context = context.withValue(key, value);
    return this;
  }

  public Context context() {
    return context;
  }
}

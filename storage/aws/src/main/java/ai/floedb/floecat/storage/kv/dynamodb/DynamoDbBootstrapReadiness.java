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
package ai.floedb.floecat.storage.kv.dynamodb;

import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.microprofile.config.ConfigProvider;

public final class DynamoDbBootstrapReadiness {
  private static final AtomicBoolean TABLE_READY = new AtomicBoolean(false);

  private DynamoDbBootstrapReadiness() {}

  public static void markReady() {
    TABLE_READY.set(true);
  }

  public static boolean isReady() {
    return TABLE_READY.get();
  }

  public static boolean shouldWaitForBootstrap() {
    var cfg = ConfigProvider.getConfig();
    String kvMode = cfg.getOptionalValue("floecat.kv", String.class).orElse("memory");
    boolean autoCreate =
        cfg.getOptionalValue("floecat.kv.auto-create", Boolean.class).orElse(false);
    return "dynamodb".equalsIgnoreCase(kvMode) && autoCreate && !TABLE_READY.get();
  }
}

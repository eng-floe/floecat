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

package ai.floedb.floecat.extensions.floedb.engine.oid;

import java.util.Locale;

/** Central place to obtain the configured {@link EngineOidGenerator}. */
public final class EngineOidGeneratorHolder {

  private static final String CONFIG_KEY = "floecat.extensions.floedb.engine.oid-generator";
  private static final String DEFAULT_STRATEGY = "default";
  private static final Object LOCK = new Object();
  private static volatile EngineOidGenerator INSTANCE;

  private EngineOidGeneratorHolder() {}

  public static EngineOidGenerator instance() {
    EngineOidGenerator generator = INSTANCE;
    if (generator == null) {
      synchronized (LOCK) {
        if (INSTANCE == null) {
          INSTANCE = init();
        }
        generator = INSTANCE;
      }
    }
    return generator;
  }

  private static EngineOidGenerator init() {
    String strategy =
        System.getProperty(CONFIG_KEY, DEFAULT_STRATEGY).trim().toLowerCase(Locale.ROOT);
    return switch (strategy) {
      case DEFAULT_STRATEGY -> new DefaultEngineOidGenerator();
      default ->
          throw new IllegalStateException("Unknown FloeDB OID generator strategy: " + strategy);
    };
  }

  /**
   * Test-only: override the system property (nullable) and reset the cached generator
   * (package-private for tests). Sets {@code INSTANCE = null} so the next {@link #instance()}
   * reinitializes with the chosen strategy.
   */
  static void resetForTests(String strategy) {
    synchronized (LOCK) {
      if (strategy == null) {
        System.clearProperty(CONFIG_KEY);
      } else {
        System.setProperty(CONFIG_KEY, strategy);
      }
      INSTANCE = null;
    }
  }
}

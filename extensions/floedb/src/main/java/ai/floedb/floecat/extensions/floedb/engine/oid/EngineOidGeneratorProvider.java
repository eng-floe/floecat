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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** CDI provider for the configured {@link EngineOidGenerator}. */
@ApplicationScoped
public final class EngineOidGeneratorProvider {

  static final String CONFIG_KEY = "floecat.extensions.floedb.engine.oid-generator";
  private static final String DEFAULT_STRATEGY = "default";
  private static volatile EngineOidGenerator instance = new DefaultEngineOidGenerator();

  private final EngineOidGenerator generator;

  @Inject
  public EngineOidGeneratorProvider(
      @ConfigProperty(name = CONFIG_KEY, defaultValue = DEFAULT_STRATEGY) String strategy) {
    this.generator = create(strategy);
    instance = this.generator;
  }

  public EngineOidGenerator get() {
    return generator;
  }

  public static EngineOidGenerator getInstance() {
    return instance;
  }

  public static void registerForTests(EngineOidGenerator generator) {
    instance = generator == null ? new DefaultEngineOidGenerator() : generator;
  }

  private static EngineOidGenerator create(String strategy) {
    String normalized = strategy == null ? DEFAULT_STRATEGY : strategy.trim().toLowerCase();
    return switch (normalized) {
      case DEFAULT_STRATEGY -> new DefaultEngineOidGenerator();
      default ->
          throw new IllegalStateException("Unknown FloeDB OID generator strategy: " + strategy);
    };
  }
}

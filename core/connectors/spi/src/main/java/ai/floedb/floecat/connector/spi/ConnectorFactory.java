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

package ai.floedb.floecat.connector.spi;

import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

public final class ConnectorFactory {
  // ServiceLoader is not thread-safe. Resolve every provider once in the class initializer
  // (single-threaded, lock-protected by the JVM) and look them up by kind afterwards. Pin to the
  // SPI classloader rather than the thread-context one so resolution does not depend on the calling
  // thread.
  private static final Map<String, ConnectorProvider> PROVIDERS =
      ServiceLoader.load(ConnectorProvider.class, ConnectorFactory.class.getClassLoader()).stream()
          .map(ServiceLoader.Provider::get)
          .collect(Collectors.toUnmodifiableMap(p -> p.kind().toLowerCase(Locale.ROOT), p -> p));

  private ConnectorFactory() {}

  /**
   * Whether this build has a provider registered for {@code kind}.
   *
   * <p>Lets a caller refuse a kind up front rather than persisting a connector that only fails much
   * later inside {@link #create} with "No ConnectorProvider for kind=...". Answered from the
   * resolved providers, not a hardcoded list, so it stays true whatever is on the classpath.
   */
  public static boolean isSupported(ConnectorConfig.Kind kind) {
    return kind != null && PROVIDERS.containsKey(kindId(kind));
  }

  private static String kindId(ConnectorConfig.Kind kind) {
    return switch (kind) {
      case ICEBERG -> "iceberg";
      case DELTA -> "delta";
      case GLUE -> "glue";
    };
  }

  public static FloecatConnector create(ConnectorConfig config) {
    String kindId = kindId(config.kind());
    ConnectorProvider p = PROVIDERS.get(kindId);
    if (p == null) {
      throw new IllegalStateException("No ConnectorProvider for kind=" + kindId);
    }
    return p.create(config);
  }
}

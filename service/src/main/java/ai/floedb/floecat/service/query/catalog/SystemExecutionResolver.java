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
package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.query.rpc.FlightEndpointRef;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

/** Resolves the execution identity and wire routing for one system-table node. */
final class SystemExecutionResolver {
  private static final Logger LOG = Logger.getLogger(SystemExecutionResolver.class);
  private static final String SYSTEM_FLIGHT_ENDPOINTS_PREFIX = "floedb.system-flight.endpoints.";

  private final FlightEndpointRef floecatFlightEndpoint;

  SystemExecutionResolver(FlightEndpointRef floecatFlightEndpoint) {
    this.floecatFlightEndpoint = floecatFlightEndpoint;
  }

  /** Routing fields served to a worker and folded into the relation possession token. */
  record SystemExecution(String backendKind, FlightEndpointRef flightEndpoint, String storagePath) {
    String tokenMaterial() {
      String endpoint =
          flightEndpoint != null
              ? flightEndpoint.getHost()
                  + ':'
                  + flightEndpoint.getPort()
                  + ':'
                  + flightEndpoint.getTls()
              : "";
      return backendKind + '\0' + endpoint + '\0' + storagePath;
    }
  }

  SystemExecution resolve(SystemTableNode node) {
    String backendKind = String.valueOf(node.backendKind());
    if (node instanceof SystemTableNode.FloeCatSystemTableNode) {
      return new SystemExecution(backendKind, floecatFlightEndpoint, "");
    }
    if (node instanceof SystemTableNode.StorageSystemTableNode storage) {
      if (storage.flightEndpoint() != null) {
        return new SystemExecution(backendKind, storage.flightEndpoint(), "");
      }
      Optional<FlightEndpointRef> configured =
          configuredEndpointForKey(storage.storageEndpointKey());
      if (configured.isPresent()) {
        return new SystemExecution(backendKind, configured.get(), "");
      }
      if (!storage.storagePath().isBlank()) {
        return new SystemExecution(backendKind, null, storage.storagePath());
      }
    }
    return new SystemExecution(backendKind, null, "");
  }

  private Optional<FlightEndpointRef> configuredEndpointForKey(String endpointKey) {
    if (endpointKey == null || endpointKey.isBlank()) {
      return Optional.empty();
    }
    String normalizedKey = endpointKey.trim();
    String prefix = SYSTEM_FLIGHT_ENDPOINTS_PREFIX + normalizedKey + ".";
    Config config = ConfigProvider.getConfig();
    Optional<String> host =
        config
            .getOptionalValue(prefix + "host", String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank());
    Optional<Integer> port =
        config.getOptionalValue(prefix + "port", Integer.class).filter(value -> value > 0);
    if (host.isEmpty() || port.isEmpty()) {
      LOG.debugf(
          "Storage endpoint key '%s' has no config at %shost/%sport; falling back to storage path",
          normalizedKey, prefix, prefix);
      return Optional.empty();
    }
    boolean tls = config.getOptionalValue(prefix + "tls", Boolean.class).orElse(false);
    return Optional.of(
        FlightEndpointRef.newBuilder().setHost(host.get()).setPort(port.get()).setTls(tls).build());
  }
}

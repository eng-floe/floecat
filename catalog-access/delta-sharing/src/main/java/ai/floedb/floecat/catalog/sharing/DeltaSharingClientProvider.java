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
package ai.floedb.floecat.catalog.sharing;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogAuthenticationScheme;
import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogClientProvider;
import ai.floedb.floecat.catalog.access.CatalogConnectionConfig;
import ai.floedb.floecat.catalog.access.CatalogProtocol;
import ai.floedb.floecat.catalog.access.ResolvedCatalogCredentials;
import ai.floedb.floecat.client.sharing.DeltaSharingClient;
import ai.floedb.floecat.client.sharing.HttpDeltaSharingClient;
import ai.floedb.floecat.http.guards.HttpEndpointGuards;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Opens Delta Sharing recipients for the catalog-access SPI. */
public final class DeltaSharingClientProvider implements CatalogClientProvider {

  private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);

  /**
   * Storage properties carried through to a vended credential.
   *
   * <p>The temporary-table-credentials response names the location and the key material and nothing
   * else, so a non-standard object store still has to be told where it is and how to address it.
   * Against real AWS these are inferable and an operator sets none of them.
   */
  private static final List<String> STORAGE_PROPERTY_KEYS =
      List.of("s3.endpoint", "s3.region", "client.region", "s3.path-style-access");

  static final String READER_FEATURES = "delta.sharing.reader-features";

  /** Seam for tests: builds the recipient without opening a connection. */
  interface ClientFactory {
    DeltaSharingClient create(
        URI endpoint,
        String bearerToken,
        Duration connectTimeout,
        Duration requestTimeout,
        List<String> readerFeatures);
  }

  private final ClientFactory clientFactory;

  public DeltaSharingClientProvider() {
    this(HttpDeltaSharingClient::new);
  }

  DeltaSharingClientProvider(ClientFactory clientFactory) {
    this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory");
  }

  @Override
  public CatalogProtocol protocol() {
    return CatalogProtocol.DELTA_SHARING;
  }

  @Override
  public CatalogClient open(
      CatalogConnectionConfig config, ResolvedCatalogCredentials resolvedCredentials) {
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(resolvedCredentials, "resolvedCredentials");
    // Cheap defence against a registry mis-dispatch, as the Unity provider makes at its entry.
    if (config.protocol() != CatalogProtocol.DELTA_SHARING) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Delta Sharing provider was asked for " + config.protocol());
    }

    // A recipient token, and nothing else. The protocol defines bearer authorization and no other
    // scheme, so anything else here is a configuration error rather than an unused field. A bearer
    // token reaches a provider as OAUTH2 carrying a token property, which is how the service
    // resolves one.
    if (config.authentication().scheme() != CatalogAuthenticationScheme.OAUTH2) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Delta Sharing authenticates with a recipient bearer token, not "
              + config.authentication().scheme());
    }
    String token = resolvedCredentials.properties().get("token");
    if (token == null || token.isBlank()) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Delta Sharing requires a resolved recipient token");
    }

    Map<String, String> properties = config.properties();
    Map<String, String> storage = new HashMap<>();
    for (String key : STORAGE_PROPERTY_KEYS) {
      String value = properties.get(key);
      if (value != null && !value.isBlank()) {
        storage.put(key, value.trim());
      }
    }
    // The same gate the Unity provider holds its own s3.endpoint to, for the same reason: this
    // vend publishes an AWS session token, and the endpoint travels on to query workers.
    try {
      HttpEndpointGuards.requireUsableStorageEndpoint(
          storage.get("s3.endpoint"), "Delta Sharing s3.endpoint");
    } catch (IllegalArgumentException refused) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION, refused.getMessage(), refused);
    }

    // Before the client exists. Arguments evaluate left to right, so resolving this inside the
    // constructor call built the transport first and leaked it when the property was malformed --
    // once per open, which is once per vend and once per validation.
    boolean strict = strictAccessModes(properties);

    // The transport is open by the time the wrapper's constructor runs, so anything it raises
    // leaks it -- once per vend and once per validation, the same window the flag above is
    // resolved early to avoid.
    DeltaSharingClient recipient;
    try {
      recipient =
          clientFactory.create(
              config.endpoint(),
              token,
              durationProperty(properties, "http.connect.ms", DEFAULT_CONNECT_TIMEOUT),
              durationProperty(properties, "http.read.ms", DEFAULT_REQUEST_TIMEOUT),
              readerFeatures(properties));
    } catch (IllegalArgumentException refused) {
      // The message as the transport wrote it. Naming a property here was wrong: the constructor
      // raises this for the endpoint gates, the two floecat.delta-sharing.* limits and a blank
      // token as well as for a reader feature, so an operator pointing at an http:// endpoint was
      // told about delta.sharing.reader-features, which they had never set.
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION, refused.getMessage(), refused);
    }
    try {
      return new DeltaSharingCatalogClient(recipient, storage, strict);
    } catch (RuntimeException | Error failure) {
      closeQuietly(recipient);
      throw failure;
    }
  }

  private static void closeQuietly(DeltaSharingClient recipient) {
    try {
      recipient.close();
    } catch (RuntimeException ignored) {
      // Closing is what releases the transport; a failure to close adds nothing to report.
    }
  }

  /**
   * Whether a table stating no access modes is read as url only, off by default.
   *
   * <p>The protocol reads an absent field as url only, and the reference server implements the
   * credential endpoint while stating nothing at all, so holding to that reading refuses every
   * table on a server that would have vended. The default asks and lets the server answer; an
   * operator who wants the protocol reading enforced without a round trip sets this.
   */
  private static boolean strictAccessModes(Map<String, String> properties) {
    String value = properties.get(DeltaSharingCatalogClient.STRICT_ACCESS_MODES);
    if (value == null || value.isBlank()) {
      return false;
    }
    String trimmed = value.trim();
    if (trimmed.equalsIgnoreCase("true")) {
      return true;
    }
    if (trimmed.equalsIgnoreCase("false")) {
      return false;
    }
    throw new CatalogAccessException(
        CatalogAccessException.Code.INVALID_CONFIGURATION,
        DeltaSharingCatalogClient.STRICT_ACCESS_MODES + " must be true or false, not " + value);
  }

  /**
   * Delta reader features this deployment can process, empty by default.
   *
   * <p>Empty rather than everything the protocol names. The capability header tells the server what
   * the client can handle, and claiming a feature the read path cannot process turns a refusal the
   * server would have made into a failure partway through a scan. An operator opts in per feature
   * once the read path is known to support it.
   *
   * <p>Enforcement is the server's. This list reaches it in {@code delta-sharing-capabilities} on
   * the metadata call, and nothing here compares it against the {@code readerFeatures} the protocol
   * action reports back. Under directory access the data read goes straight to object storage with
   * the vended credential, so there is no later point at which the server could refuse -- which
   * means a server that ignores the header is not caught, and a table needing more than the opt-in
   * would be read rather than rejected. Checking it locally is a policy this does not yet take.
   */
  private static List<String> readerFeatures(Map<String, String> properties) {
    String configured = properties.get(READER_FEATURES);
    if (configured == null || configured.isBlank()) {
      return List.of();
    }
    List<String> features = new ArrayList<>();
    for (String feature : configured.split(",")) {
      String trimmed = feature.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      // Not checked here. The transport validates each token where it interpolates it into the
      // capability header, which is the only place the shape matters and the one place every
      // construction path goes through; open() turns that refusal into this property's name.
      features.add(trimmed);
    }
    return List.copyOf(features);
  }

  private static Duration durationProperty(
      Map<String, String> properties, String key, Duration fallback) {
    String value = properties.get(key);
    if (value == null || value.isBlank()) {
      return fallback;
    }
    long millis;
    try {
      millis = Long.parseLong(value.trim());
    } catch (NumberFormatException notANumber) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          key + " must be a number of milliseconds, not " + value);
    }
    if (millis <= 0) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION, key + " must be above zero");
    }
    return Duration.ofMillis(millis);
  }
}

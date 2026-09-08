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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogAuthentication;
import ai.floedb.floecat.catalog.access.CatalogAuthenticationScheme;
import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogClientProvider;
import ai.floedb.floecat.catalog.access.CatalogConnectionConfig;
import ai.floedb.floecat.catalog.access.CatalogProtocol;
import ai.floedb.floecat.catalog.access.ResolvedCatalogCredentials;
import ai.floedb.floecat.client.sharing.DeltaSharingClient;
import ai.floedb.floecat.http.guards.HttpEndpointGuards;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

class DeltaSharingClientProviderTest {

  @Test
  void serviceLoaderFindsTheProviderForItsProtocol() {
    assertThat(ServiceLoader.load(CatalogClientProvider.class))
        .filteredOn(provider -> provider.protocol() == CatalogProtocol.DELTA_SHARING)
        .hasSize(1)
        .allSatisfy(
            provider -> assertThat(provider).isInstanceOf(DeltaSharingClientProvider.class));
  }

  @Test
  void aResolvedTokenAndTheDefaultTimeoutsReachTheRecipient() {
    Recording recording = new Recording();
    try (CatalogClient client =
        new DeltaSharingClientProvider(recording)
            .open(config(Map.of()), credentials("recipient"))) {
      assertThat(client).isInstanceOf(DeltaSharingCatalogClient.class);
    }
    assertThat(recording.endpoint).isEqualTo(URI.create("https://sharing.example/delta-sharing"));
    assertThat(recording.bearerToken).isEqualTo("recipient");
    assertThat(recording.connectTimeout).isEqualTo(Duration.ofSeconds(10));
    assertThat(recording.requestTimeout).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void noReaderFeatureIsClaimedUnlessAnOperatorOptsIn() {
    Recording recording = new Recording();
    new DeltaSharingClientProvider(recording).open(config(Map.of()), credentials("recipient"));
    assertThat(recording.readerFeatures).isEmpty();

    Recording opted = new Recording();
    new DeltaSharingClientProvider(opted)
        .open(
            config(Map.of("delta.sharing.reader-features", "deletionVectors, columnMapping")),
            credentials("recipient"));
    assertThat(opted.readerFeatures).containsExactly("deletionVectors", "columnMapping");
  }

  @Test
  void onlyTheStoragePropertiesAnObjectStoreNeedsAreCarriedThrough() {
    Recording recording = new Recording();
    DeltaSharingCatalogClient client =
        (DeltaSharingCatalogClient)
            new DeltaSharingClientProvider(recording)
                .open(
                    config(
                        Map.of(
                            "s3.endpoint", "https://minio.internal:9000",
                            "s3.region", "us-east-1",
                            "unrelated", "value")),
                    credentials("recipient"));
    assertThat(client.storageProperties())
        .containsOnlyKeys("s3.endpoint", "s3.region")
        .containsEntry("s3.region", "us-east-1");
  }

  @Test
  void theStrictAccessModeSettingIsOffUnlessAnOperatorTurnsItOn() {
    Recording recording = new Recording();
    DeltaSharingCatalogClient asking =
        (DeltaSharingCatalogClient)
            new DeltaSharingClientProvider(recording)
                .open(config(Map.of()), credentials("recipient"));
    assertThat(asking.strictAccessModes()).isFalse();

    DeltaSharingCatalogClient strict =
        (DeltaSharingCatalogClient)
            new DeltaSharingClientProvider(recording)
                .open(
                    config(Map.of(DeltaSharingCatalogClient.STRICT_ACCESS_MODES, "TRUE")),
                    credentials("recipient"));
    assertThat(strict.strictAccessModes()).isTrue();
  }

  @Test
  void aStrictAccessModeSettingThatIsNeitherTrueNorFalseIsReported() {
    assertThatThrownBy(
            () ->
                new DeltaSharingClientProvider(new Recording())
                    .open(
                        config(Map.of(DeltaSharingCatalogClient.STRICT_ACCESS_MODES, "yes")),
                        credentials("recipient")))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure -> {
              assertThat(failure.code())
                  .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION);
              assertThat(failure.getMessage())
                  .contains(DeltaSharingCatalogClient.STRICT_ACCESS_MODES);
            });
  }

  /**
   * The storage endpoint is held to the gate the Unity provider uses, for the same reason: this
   * vend publishes an AWS session token, which travels in a header and is replayable, and {@code
   * s3.endpoint} travels on to query workers.
   */
  @Test
  void refusesAStorageEndpointThatIsCleartextOrNamesAnAddressClassThatIsNotAllowed() {
    DeltaSharingClientProvider provider = new DeltaSharingClientProvider(new Recording());

    for (String refused :
        List.of(
            "http://minio.internal:9000", "https://169.254.169.254", "not-a-uri", "/no/scheme")) {
      assertThatThrownBy(
              () -> provider.open(config(Map.of("s3.endpoint", refused)), credentials("recipient")))
          .describedAs("%s", refused)
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code())
                      .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION));
    }

    // HTTPS needs no opt-in.
    provider
        .open(config(Map.of("s3.endpoint", "https://storage.example")), credentials("recipient"))
        .close();

    System.setProperty(HttpEndpointGuards.ALLOW_CLEARTEXT_S3_PROPERTY, "true");
    try {
      provider
          .open(
              config(Map.of("s3.endpoint", "http://minio.internal:9000")), credentials("recipient"))
          .close();
    } finally {
      System.clearProperty(HttpEndpointGuards.ALLOW_CLEARTEXT_S3_PROPERTY);
    }
  }

  /**
   * The transport's own message survives. Naming a property here was wrong: its constructor raises
   * IllegalArgumentException for the endpoint gates and the response limits as well as for a reader
   * feature, so an operator pointing at an http:// endpoint was told about a property they had
   * never set.
   */
  @Test
  void aTransportRefusalKeepsItsOwnMessage() {
    DeltaSharingClientProvider.ClientFactory refusing =
        (endpoint, token, connect, read, features) -> {
          throw new IllegalArgumentException("Delta reader feature must be letters: x;y");
        };
    assertThatThrownBy(
            () ->
                new DeltaSharingClientProvider(refusing)
                    .open(
                        config(Map.of(DeltaSharingClientProvider.READER_FEATURES, "x;y")),
                        credentials("recipient")))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure -> {
              assertThat(failure.code())
                  .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION);
              assertThat(failure.getMessage()).contains("Delta reader feature must be letters");
              assertThat(failure.getMessage())
                  .doesNotContain(DeltaSharingClientProvider.READER_FEATURES);
            });
  }

  /** A registry mis-dispatch is cheap to refuse, and the Unity provider refuses it too. */
  @Test
  void aConfigForAnotherProtocolIsRefused() {
    assertThatThrownBy(
            () ->
                new DeltaSharingClientProvider(new Recording())
                    .open(
                        new CatalogConnectionConfig(
                            CatalogProtocol.UNITY_CATALOG,
                            URI.create("https://sharing.example/delta-sharing"),
                            Map.of(),
                            new CatalogAuthentication(
                                CatalogAuthenticationScheme.OAUTH2, Map.of())),
                        credentials("recipient")))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure ->
                assertThat(failure.code())
                    .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION));
  }

  @Test
  void aSchemeOtherThanBearerIsAConfigurationError() {
    assertThatThrownBy(
            () ->
                new DeltaSharingClientProvider(new Recording())
                    .open(
                        new CatalogConnectionConfig(
                            CatalogProtocol.DELTA_SHARING,
                            URI.create("https://sharing.example/delta-sharing"),
                            Map.of(),
                            CatalogAuthentication.none()),
                        ResolvedCatalogCredentials.none()))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure -> {
              assertThat(failure.code())
                  .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION);
              assertThat(failure.getMessage()).contains("NONE");
            });
  }

  @Test
  void anUnresolvedTokenIsAConfigurationErrorRatherThanAnAnonymousRequest() {
    assertThatThrownBy(
            () ->
                new DeltaSharingClientProvider(new Recording())
                    .open(config(Map.of()), ResolvedCatalogCredentials.none()))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure ->
                assertThat(failure.code())
                    .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION));
  }

  /**
   * Arguments evaluate left to right, so resolving the flag inside the constructor call built the
   * transport first and leaked it when the property was malformed -- once per open, which is once
   * per vend and once per validation.
   */
  @Test
  void aMalformedStrictSettingDoesNotLeaveAClientBehind() {
    Recording recording = new Recording();
    assertThatThrownBy(
            () ->
                new DeltaSharingClientProvider(recording)
                    .open(
                        config(Map.of(DeltaSharingCatalogClient.STRICT_ACCESS_MODES, "yes")),
                        credentials("recipient")))
        .isInstanceOf(CatalogAccessException.class);
    assertThat(recording.created).isFalse();
  }

  @Test
  void anUnreadableTimeoutIsReportedRatherThanSilentlyDefaulted() {
    for (String bad : List.of("soon", "0", "-1")) {
      assertThatThrownBy(
              () ->
                  new DeltaSharingClientProvider(new Recording())
                      .open(config(Map.of("http.read.ms", bad)), credentials("recipient")))
          .describedAs("http.read.ms=%s", bad)
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code())
                      .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION));
    }
  }

  private static CatalogConnectionConfig config(Map<String, String> properties) {
    return new CatalogConnectionConfig(
        CatalogProtocol.DELTA_SHARING,
        URI.create("https://sharing.example/delta-sharing"),
        properties,
        new CatalogAuthentication(CatalogAuthenticationScheme.OAUTH2, Map.of()));
  }

  private static ResolvedCatalogCredentials credentials(String token) {
    return new ResolvedCatalogCredentials(Map.of("token", token), Map.of(), null);
  }

  /** Captures what the provider built the recipient from, without opening a connection. */
  private static final class Recording implements DeltaSharingClientProvider.ClientFactory {
    private URI endpoint;
    private String bearerToken;
    private Duration connectTimeout;
    private Duration requestTimeout;
    private List<String> readerFeatures;
    private boolean created;

    @Override
    public DeltaSharingClient create(
        URI endpoint,
        String bearerToken,
        Duration connectTimeout,
        Duration requestTimeout,
        List<String> readerFeatures) {
      this.endpoint = endpoint;
      this.bearerToken = bearerToken;
      this.connectTimeout = connectTimeout;
      this.requestTimeout = requestTimeout;
      this.readerFeatures = readerFeatures;
      this.created = true;
      return mock(DeltaSharingClient.class);
    }
  }
}

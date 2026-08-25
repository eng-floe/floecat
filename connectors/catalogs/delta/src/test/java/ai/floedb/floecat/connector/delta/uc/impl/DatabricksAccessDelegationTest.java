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

package ai.floedb.floecat.connector.delta.uc.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.DatabricksAccessDelegation;
import ai.floedb.floecat.connector.spi.IcebergAccessDelegation;
import ai.floedb.floecat.connector.spi.SourceCatalogVending;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DatabricksAccessDelegationTest {

  private static ConnectorConfig config(ConnectorConfig.Kind kind, Map<String, String> options) {
    return new ConnectorConfig(kind, "display", "https://example", options, null);
  }

  @Test
  void unityConnectorOptsInWhenFlagIsTruthy() {
    for (String truthy : new String[] {"true", "1", "yes", "vended-credentials", "TRUE"}) {
      var cfg =
          config(
              ConnectorConfig.Kind.UNITY, Map.of(DatabricksAccessDelegation.VEND_OPTION, truthy));
      assertThat(DatabricksAccessDelegation.declaresVendedCredentials(cfg)).as(truthy).isTrue();
    }
  }

  @Test
  void deltaKindAlsoHonorsTheOptIn() {
    var cfg =
        config(ConnectorConfig.Kind.DELTA, Map.of(DatabricksAccessDelegation.VEND_OPTION, "true"));
    assertThat(DatabricksAccessDelegation.declaresVendedCredentials(cfg)).isTrue();
  }

  @Test
  void nonUnityDeltaSourcesIgnoreTheOptIn() {
    for (String source : new String[] {"glue", "filesystem"}) {
      var cfg =
          config(
              ConnectorConfig.Kind.DELTA,
              Map.of("delta.source", source, DatabricksAccessDelegation.VEND_OPTION, "true"));
      assertThat(DatabricksAccessDelegation.declaresVendedCredentials(cfg)).as(source).isFalse();
    }
  }

  @Test
  void absentOrFalsyFlagIsNotDeclared() {
    assertThat(
            DatabricksAccessDelegation.declaresVendedCredentials(
                config(ConnectorConfig.Kind.UNITY, Map.of())))
        .isFalse();
    assertThat(
            DatabricksAccessDelegation.declaresVendedCredentials(
                config(
                    ConnectorConfig.Kind.UNITY,
                    Map.of(DatabricksAccessDelegation.VEND_OPTION, "false"))))
        .isFalse();
  }

  @Test
  void icebergKindIgnoresTheDatabricksFlag() {
    // The Databricks opt-in must not fire on an Iceberg connector; that path uses the delegation
    // header instead. A stray copy of the property is simply ignored.
    var cfg =
        config(
            ConnectorConfig.Kind.ICEBERG, Map.of(DatabricksAccessDelegation.VEND_OPTION, "true"));
    assertThat(DatabricksAccessDelegation.declaresVendedCredentials(cfg)).isFalse();
  }

  @Test
  void neutralDispatcherAcceptsBothFormats() {
    // Unity via the opt-in option...
    assertThat(
            SourceCatalogVending.declaresVendedCredentials(
                config(
                    ConnectorConfig.Kind.UNITY,
                    Map.of(DatabricksAccessDelegation.VEND_OPTION, "true"))))
        .isTrue();
    // ...and Iceberg via its access-delegation header.
    assertThat(
            SourceCatalogVending.declaresVendedCredentials(
                config(
                    ConnectorConfig.Kind.ICEBERG,
                    Map.of(IcebergAccessDelegation.HEADER_PROPERTY, "vended-credentials"))))
        .isTrue();
    // A Delta connector that opted into neither declares nothing.
    assertThat(
            SourceCatalogVending.declaresVendedCredentials(
                config(ConnectorConfig.Kind.DELTA, Map.of())))
        .isFalse();
  }

  @Test
  void neutralDispatcherDoesNotCrossFormatBoundaries() {
    assertThat(
            SourceCatalogVending.declaresVendedCredentials(
                config(
                    ConnectorConfig.Kind.UNITY,
                    Map.of(IcebergAccessDelegation.HEADER_PROPERTY, "vended-credentials"))))
        .isFalse();
    assertThat(
            SourceCatalogVending.declaresVendedCredentials(
                config(
                    ConnectorConfig.Kind.ICEBERG,
                    Map.of(DatabricksAccessDelegation.VEND_OPTION, "true"))))
        .isFalse();
  }
}

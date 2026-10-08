/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.account.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class AwsServicePrincipalConfigurationTest {
  private static final String PROPERTY = "floecat.catalog-integrations.aws.service-principal-arn";
  private static final String EXPLICIT = "FLOECAT_CATALOG_INTEGRATIONS_AWS_SERVICE_PRINCIPAL_ARN";

  @Test
  void explicitPrincipalOverridesTheIrsaRole() throws Exception {
    assertThat(
            resolvedPrincipal(
                Map.of(
                    EXPLICIT,
                    "arn:aws:iam::123456789012:role/explicit",
                    "AWS_ROLE_ARN",
                    "arn:aws:iam::123456789012:role/irsa")))
        .contains("arn:aws:iam::123456789012:role/explicit");
  }

  @Test
  void usesTheIrsaRoleWhenNoExplicitPrincipalIsConfigured() throws Exception {
    assertThat(resolvedPrincipal(Map.of("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/irsa")))
        .contains("arn:aws:iam::123456789012:role/irsa");
  }

  @Test
  void remainsUnsetForAStandaloneDeploymentWithoutAws() throws Exception {
    assertThat(resolvedPrincipal(Map.of())).isEmpty();
  }

  private static Optional<String> resolvedPrincipal(Map<String, String> overrides)
      throws IOException {
    var properties = new Properties();
    try (var input = Files.newInputStream(Path.of("src/main/resources/application.properties"))) {
      properties.load(input);
    }
    var values = new HashMap<String, String>();
    properties.forEach((key, value) -> values.put((String) key, (String) value));
    values.putAll(overrides);
    var config =
        new SmallRyeConfigBuilder()
            .addDefaultInterceptors()
            .withSources(new PropertiesConfigSource(values, "test", 500))
            .build();
    return config.getOptionalValue(PROPERTY, String.class);
  }
}

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

package ai.floedb.floecat.connector.iceberg.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IcebergConnectorFactoryTest {

  @Test
  void createsRestConnectorWhenSourceRest() {
    var source = IcebergConnectorFactory.selectSource(Map.of("iceberg.source", "rest"));
    assertEquals(IcebergConnectorFactory.IcebergSource.REST, source);
  }

  @Test
  void createsGlueConnectorWhenSourceGlue() {
    var source = IcebergConnectorFactory.selectSource(Map.of("iceberg.source", "glue"));
    assertEquals(IcebergConnectorFactory.IcebergSource.GLUE, source);
  }

  @Test
  void filesystemSourceRequiresUri() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IcebergConnectorFactory.validateOptions(
                    IcebergConnectorFactory.IcebergSource.FILESYSTEM, "", Map.of(), "none"));
    assertTrue(ex.getMessage().contains("uri"));
  }

  @Test
  void filesystemS3RejectsUnsupportedStorageAuthScheme() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IcebergConnectorFactory.validateOptions(
                    IcebergConnectorFactory.IcebergSource.FILESYSTEM,
                    "s3://bucket/table/metadata/00001.metadata.json",
                    Map.of("iceberg.source", "filesystem"),
                    "oauth2"));
    assertTrue(ex.getMessage().contains("Unsupported filesystem storage auth scheme"));
  }

  @Test
  void buildRestPropsMirrorsS3RegionIntoClientRegion() throws Exception {
    Map<String, String> props =
        IcebergConnectorFactory.buildCatalogProperties(
            "https://glue.us-east-2.amazonaws.com/iceberg/",
            IcebergConnectorFactory.buildBaseIcebergProperties(
                Map.of("iceberg.source", "glue", "s3.region", "us-east-2")));

    assertEquals("us-east-2", props.get("s3.region"));
    assertEquals("us-east-2", props.get("client.region"));
  }

  @Test
  void buildRestPropsPreservesExplicitClientRegion() throws Exception {
    Map<String, String> props =
        IcebergConnectorFactory.buildCatalogProperties(
            "https://glue.us-east-2.amazonaws.com/iceberg/",
            IcebergConnectorFactory.buildBaseIcebergProperties(
                Map.of(
                    "iceberg.source", "glue",
                    "s3.region", "us-east-2",
                    "client.region", "us-west-2")));

    assertEquals("us-east-2", props.get("s3.region"));
    assertEquals("us-west-2", props.get("client.region"));
  }

  @Test
  void applyAuthAwsSigV4HandlesNullAuthProps() throws Exception {
    Method method =
        IcebergConnectorFactory.class.getDeclaredMethod(
            "applyCatalogAuth", Map.class, String.class, Map.class);
    method.setAccessible(true);
    Map<String, String> props = new HashMap<>();
    props.put("s3.region", "us-east-2");

    assertDoesNotThrow(() -> method.invoke(null, props, "aws-sigv4", null));
    assertEquals("sigv4", props.get("rest.auth.type"));
    assertEquals("glue", props.get("rest.signing-name"));
    assertEquals("us-east-2", props.get("rest.signing-region"));
  }

  @Test
  void applyAuthOauth2NullAuthPropsFailsWithClearMessage() throws Exception {
    Method method =
        IcebergConnectorFactory.class.getDeclaredMethod(
            "applyCatalogAuth", Map.class, String.class, Map.class);
    method.setAccessible(true);
    Map<String, String> props = new HashMap<>();

    InvocationTargetException ex =
        assertThrows(
            InvocationTargetException.class, () -> method.invoke(null, props, "oauth2", null));
    Throwable cause = ex.getCause();
    assertNotNull(cause);
    assertEquals(NullPointerException.class, cause.getClass());
    assertTrue(cause.getMessage().contains("authProps.token required for oauth2"));
  }

  @Test
  void buildStoragePropertiesCarriesResolvedAwsCredentialsForFilesystem() {
    Map<String, String> props =
        IcebergConnectorFactory.buildStorageProperties(
            IcebergConnectorFactory.buildBaseIcebergProperties(
                Map.of(
                    "iceberg.source", "filesystem",
                    "s3.region", "us-east-1",
                    "external.namespace", "raw")),
            "aws-sigv4",
            Map.of("aws.profile", "dev-profile"));

    assertEquals("us-east-1", props.get("s3.region"));
    assertEquals("us-east-1", props.get("client.region"));
    assertEquals("dev-profile", props.get("aws.profile"));
    assertEquals("org.apache.iceberg.aws.s3.S3FileIO", props.get("io-impl"));
    assertEquals("raw", props.get("external.namespace"));
  }

  @Test
  void buildStoragePropertiesLeavesOauthTokenOutOfFilesystemIoProps() {
    Map<String, String> props =
        IcebergConnectorFactory.buildStorageProperties(
            IcebergConnectorFactory.buildBaseIcebergProperties(
                Map.of("iceberg.source", "filesystem", "s3.region", "us-east-1")),
            "oauth2",
            Map.of("token", "secret-token"));

    assertEquals("us-east-1", props.get("s3.region"));
    assertEquals("us-east-1", props.get("client.region"));
    assertTrue(!props.containsKey("token"));
  }

  @Test
  void filesystemStoragePropsIncludeResolvedAwsCredentials() {
    ConnectorConfig resolved =
        CredentialResolverSupport.apply(
            new ConnectorConfig(
                ConnectorConfig.Kind.ICEBERG,
                "fs-iceberg",
                "s3://bucket/table/metadata/00001.metadata.json",
                Map.of(
                    "iceberg.source", "filesystem",
                    "s3.region", "us-east-1",
                    "warehouse", "ignored"),
                new ConnectorConfig.Auth("aws-sigv4", Map.of(), Map.of())),
            AuthCredentials.newBuilder()
                .setAws(
                    AuthCredentials.AwsCredentials.newBuilder()
                        .setAccessKeyId("akid")
                        .setSecretAccessKey("secret")
                        .setSessionToken("session"))
                .build());

    Map<String, String> props =
        IcebergConnectorFactory.buildStorageProperties(
            IcebergConnectorFactory.buildBaseIcebergProperties(resolved.options()),
            resolved.auth().scheme(),
            resolved.auth().props());

    assertEquals("akid", props.get("s3.access-key-id"));
    assertEquals("secret", props.get("s3.secret-access-key"));
    assertEquals("session", props.get("s3.session-token"));
    assertEquals("org.apache.iceberg.aws.s3.S3FileIO", props.get("io-impl"));
    assertEquals("us-east-1", props.get("s3.region"));
    assertEquals("us-east-1", props.get("client.region"));
    assertEquals("ignored", props.get("warehouse"));
  }

  @Test
  void polarisRestCatalogPropsUseOauthTokenWithoutStaticStorageCredentials() {
    Map<String, String> props =
        IcebergConnectorFactory.buildCatalogProperties(
            "http://polaris:8181/api/catalog",
            IcebergConnectorFactory.buildBaseIcebergProperties(
                Map.of(
                    "iceberg.source", "rest",
                    "rest.flavor", "polaris",
                    "warehouse", "quickstart_catalog",
                    "s3.endpoint", "http://localstack:4566",
                    "s3.path-style-access", "true",
                    "s3.region", "us-east-1")));

    IcebergConnectorFactory.applyCatalogAuth(props, "oauth2", Map.of("token", "oauth-token"));
    IcebergConnectorFactory.applyStorageAuth(props, "oauth2", Map.of("token", "oauth-token"));
    IcebergConnectorFactory.applyAccessDelegation(
        props, IcebergConnectorFactory.IcebergSource.REST);

    assertEquals("oauth2", props.get("rest.auth.type"));
    assertEquals("oauth-token", props.get("token"));
    assertNull(props.get("oauth2-server-uri"));
    assertEquals("vended-credentials", props.get("header.X-Iceberg-Access-Delegation"));
    assertEquals("polaris", props.get("rest.flavor"));
    assertEquals("quickstart_catalog", props.get("warehouse"));
    assertEquals("http://localstack:4566", props.get("s3.endpoint"));
    assertEquals("true", props.get("s3.path-style-access"));
    assertEquals("us-east-1", props.get("s3.region"));
    assertEquals("us-east-1", props.get("client.region"));
    assertNull(props.get("s3.access-key-id"));
    assertNull(props.get("s3.secret-access-key"));
    assertNull(props.get("s3.session-token"));
  }

  @Test
  void restAccessDelegationDoesNotOverrideExplicitHeader() {
    Map<String, String> props =
        new HashMap<>(
            IcebergConnectorFactory.buildCatalogProperties(
                "http://polaris:8181/api/catalog",
                IcebergConnectorFactory.buildBaseIcebergProperties(
                    Map.of("iceberg.source", "rest", "warehouse", "quickstart_catalog"))));
    props.put("header.x-iceberg-access-delegation", "remote-signing");

    IcebergConnectorFactory.applyAccessDelegation(
        props, IcebergConnectorFactory.IcebergSource.REST);

    assertEquals("remote-signing", props.get("header.x-iceberg-access-delegation"));
    assertNull(props.get("header.X-Iceberg-Access-Delegation"));
  }

  @Test
  void polarisRestCatalogPropsCarryExplicitOauth2ServerUri() {
    Map<String, String> props =
        IcebergConnectorFactory.buildCatalogProperties(
            "http://polaris:8181/api/catalog",
            IcebergConnectorFactory.buildBaseIcebergProperties(
                Map.of(
                    "iceberg.source", "rest",
                    "rest.flavor", "polaris",
                    "warehouse", "quickstart_catalog")));

    IcebergConnectorFactory.applyCatalogAuth(
        props,
        "oauth2",
        Map.of(
            "token", "oauth-token",
            "oauth2-server-uri", "http://polaris:8181/api/catalog/v1/oauth/tokens"));

    assertEquals("oauth2", props.get("rest.auth.type"));
    assertEquals("oauth-token", props.get("token"));
    assertEquals("http://polaris:8181/api/catalog/v1/oauth/tokens", props.get("oauth2-server-uri"));
  }

  @Test
  void glueCatalogDoesNotRequestVendedCredentials() {
    Map<String, String> props =
        new HashMap<>(
            IcebergConnectorFactory.buildCatalogProperties(
                "https://glue.us-east-1.amazonaws.com/iceberg",
                IcebergConnectorFactory.buildBaseIcebergProperties(
                    Map.of("iceberg.source", "glue", "warehouse", "ignored"))));

    IcebergConnectorFactory.applyAccessDelegation(
        props, IcebergConnectorFactory.IcebergSource.GLUE);

    assertNull(props.get("header.X-Iceberg-Access-Delegation"));
  }
}

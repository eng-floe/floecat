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

import ai.floedb.floecat.connector.common.auth.RefreshingAwsCredentialsProviderRegistry;
import ai.floedb.floecat.connector.common.auth.RegistryBackedAwsCredentialsProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.RESTSigV4AuthSession;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import software.amazon.awssdk.auth.signer.Aws4Signer;

/** Uses catalog-scoped AWS credentials for REST signing without changing FileIO credentials. */
public final class CatalogSigV4AuthManager implements AuthManager {
  private static final String CLIENT_CREDENTIALS_PROVIDER = "client.credentials-provider";
  private static final String CLIENT_CREDENTIALS_PROVIDER_PREFIX =
      CLIENT_CREDENTIALS_PROVIDER + ".";

  private final Aws4Signer signer = Aws4Signer.create();
  private final String name;
  private volatile AuthManager delegate;
  private Map<String, String> catalogProperties = Map.of();

  public CatalogSigV4AuthManager(String name) {
    this.name = name;
  }

  @Override
  public AuthSession initSession(RESTClient initClient, Map<String, String> properties) {
    Map<String, String> authProperties = catalogAuthProperties(properties);
    return new RESTSigV4AuthSession(
        signer,
        delegate(properties).initSession(initClient, authProperties),
        new AwsProperties(authProperties));
  }

  @Override
  public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
    catalogProperties = properties;
    Map<String, String> authProperties = catalogAuthProperties(properties);
    return new RESTSigV4AuthSession(
        signer,
        delegate(properties).catalogSession(sharedClient, authProperties),
        new AwsProperties(authProperties));
  }

  @Override
  public AuthSession contextualSession(SessionCatalog.SessionContext context, AuthSession parent) {
    RESTSigV4AuthSession sigV4Parent = requireSigV4Parent(parent);
    Map<String, String> contextProperties =
        RESTUtil.merge(
            catalogProperties,
            RESTUtil.merge(
                Optional.ofNullable(context.properties()).orElseGet(Map::of),
                Optional.ofNullable(context.credentials()).orElseGet(Map::of)));
    Map<String, String> authProperties = catalogAuthProperties(contextProperties);
    return new RESTSigV4AuthSession(
        signer,
        delegate().contextualSession(context, sigV4Parent.delegate()),
        new AwsProperties(authProperties));
  }

  @Override
  public AuthSession tableSession(
      TableIdentifier table, Map<String, String> properties, AuthSession parent) {
    RESTSigV4AuthSession sigV4Parent = requireSigV4Parent(parent);
    Map<String, String> authProperties =
        catalogAuthProperties(RESTUtil.merge(catalogProperties, properties));
    return new RESTSigV4AuthSession(
        signer,
        delegate().tableSession(table, properties, sigV4Parent.delegate()),
        new AwsProperties(authProperties));
  }

  @Override
  public void close() {
    AuthManager current = delegate;
    if (current != null) {
      current.close();
    }
  }

  private AuthManager delegate(Map<String, String> properties) {
    AuthManager current = delegate;
    if (current != null) {
      return current;
    }
    synchronized (this) {
      current = delegate;
      if (current == null) {
        String delegateType =
            properties.getOrDefault(
                AuthProperties.SIGV4_DELEGATE_AUTH_TYPE,
                AuthProperties.SIGV4_DELEGATE_AUTH_TYPE_DEFAULT);
        if (AuthProperties.AUTH_TYPE_SIGV4.equals(delegateType)
            || CatalogSigV4AuthManager.class.getName().equals(delegateType)) {
          throw new IllegalArgumentException(
              "Catalog SigV4 auth cannot delegate to " + delegateType);
        }
        Map<String, String> delegateProperties = new HashMap<>(properties);
        delegateProperties.put(AuthProperties.AUTH_TYPE, delegateType);
        current = AuthManagers.loadAuthManager(name, delegateProperties);
        delegate = current;
      }
    }
    return current;
  }

  private AuthManager delegate() {
    AuthManager current = delegate;
    if (current == null) {
      throw new IllegalStateException("Catalog SigV4 auth manager is not initialized");
    }
    return current;
  }

  static Map<String, String> catalogAuthProperties(Map<String, String> properties) {
    Map<String, String> authProperties = new HashMap<>(properties);
    authProperties.remove(CLIENT_CREDENTIALS_PROVIDER);
    authProperties.keySet().removeIf(key -> key.startsWith(CLIENT_CREDENTIALS_PROVIDER_PREFIX));
    String providerId =
        authProperties.get(RefreshingAwsCredentialsProviderRegistry.CATALOG_OPTION_PROVIDER_ID);
    if (providerId == null || providerId.isBlank()) {
      return Map.copyOf(authProperties);
    }
    authProperties.put(
        CLIENT_CREDENTIALS_PROVIDER, RegistryBackedAwsCredentialsProvider.class.getName());
    authProperties.put(
        CLIENT_CREDENTIALS_PROVIDER_PREFIX
            + RefreshingAwsCredentialsProviderRegistry.PROPERTY_PROVIDER_ID,
        providerId);
    authProperties.put(
        CLIENT_CREDENTIALS_PROVIDER_PREFIX
            + RefreshingAwsCredentialsProviderRegistry.PROPERTY_CREDENTIAL_SCOPE,
        "catalog");
    authProperties.remove("rest.access-key-id");
    authProperties.remove("rest.secret-access-key");
    authProperties.remove("rest.session-token");
    return Map.copyOf(authProperties);
  }

  private static RESTSigV4AuthSession requireSigV4Parent(AuthSession parent) {
    if (!(parent instanceof RESTSigV4AuthSession sigV4Parent)) {
      throw new IllegalStateException("Parent session is not SigV4: " + parent);
    }
    return sigV4Parent;
  }
}

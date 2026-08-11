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

/**
 * Reads the Iceberg REST access-delegation setting off a connector's configuration.
 *
 * <p>Two independent gates depend on this answer and must agree. The storage service decides
 * whether to <em>attempt</em> source-catalog vending for a table with no matching storage
 * authority; the reconciler's config resolver decides whether to <em>absorb</em> the resulting
 * missing-authority error and proceed with the untouched connector config. If the two disagree --
 * one accepting a token or a spelling the other rejects -- a connector attempts vending on one side
 * without having its failure absorbed on the other, and the mismatch surfaces far away as an opaque
 * FileIO read error rather than as a configuration problem. Hence one parser, in the module that
 * owns {@link ConnectorConfig}, rather than a copy on each side kept in lockstep by hand.
 */
public final class IcebergAccessDelegation {

  /**
   * The access-delegation header as it appears in connector properties.
   *
   * <p>Set on a connector with {@code --props
   * header.X-Iceberg-Access-Delegation=vended-credentials}. Callers read this rather than the
   * derived catalog properties because both gates run before the connector factory builds them;
   * {@code IcebergConnectorFactory} reads the same key downstream.
   */
  public static final String HEADER_PROPERTY = "header.X-Iceberg-Access-Delegation";

  private static final String VENDED_CREDENTIALS = "vended-credentials";

  private IcebergAccessDelegation() {}

  /**
   * Whether {@code config} asks its catalog to vend storage credentials.
   *
   * <p>Both configuration routes count. Options carry {@link #HEADER_PROPERTY} directly, while
   * {@code auth().headerHints()} is turned into the same {@code header.<name>} catalog property by
   * IcebergConnectorFactory -- so reading options alone misses a connector configured through
   * header hints, which then falls through to storage-authority vending despite having asked for
   * delegation.
   */
  public static boolean declaresVendedCredentials(ConnectorConfig config) {
    if (config == null) {
      return false;
    }
    if (headerDeclaresVendedCredentials(config.options().get(HEADER_PROPERTY))) {
      return true;
    }
    if (config.auth() == null) {
      return false;
    }
    // Header hints are keyed by bare header name; the factory prefixes them with "header.".
    return config.auth().headerHints().entrySet().stream()
        .anyMatch(
            e ->
                HEADER_PROPERTY.equalsIgnoreCase("header." + e.getKey())
                    && headerDeclaresVendedCredentials(e.getValue()));
  }

  /**
   * Whether a raw header value asks specifically for <em>vended credentials</em>.
   *
   * <p>The value is not a boolean. {@code remote-signing} is equally valid and means the catalog
   * signs requests rather than returning credentials, so treating any non-blank value as
   * "credentials are coming" would swallow a genuine missing-authority failure and leave the
   * client's FileIO with nothing -- surfacing later as a far less diagnostic read error. The spec
   * also permits a comma-separated list, so each token is examined.
   */
  public static boolean headerDeclaresVendedCredentials(String headerValue) {
    if (headerValue == null || headerValue.isBlank()) {
      return false;
    }
    for (String token : headerValue.split(",")) {
      if (VENDED_CREDENTIALS.equals(token.trim().toLowerCase(Locale.ROOT).replace('_', '-'))) {
        return true;
      }
    }
    return false;
  }
}

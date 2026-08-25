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
 * Reads the vend-credentials opt-in off a Databricks / Unity Catalog connector's configuration.
 *
 * <p>The Unity Catalog analog of Iceberg's {@link IcebergAccessDelegation} is the <a
 * href="https://docs.databricks.com/api/workspace/temporarytablecredentials">temporary table
 * credentials</a> API: given a table id, UC hands back short-lived cloud credentials scoped to that
 * table's storage location. Unlike Iceberg REST there is no request header that declares the intent
 * -- vending is a server capability gated by the {@code EXTERNAL USE SCHEMA} privilege -- so the
 * connector opts in explicitly through a property instead.
 *
 * <p>The opt-in is required rather than implicit for the same reason Iceberg's header is: the
 * reconciler absorbs a missing-storage-authority error <em>only</em> when vending was declared.
 * Making it implicit for every Unity connector would silently swallow a genuinely misconfigured
 * storage authority and resurface it far away as an opaque FileIO read failure. Both gates read
 * this one parser so they cannot drift apart -- the same invariant {@link IcebergAccessDelegation}
 * documents.
 */
public final class DatabricksAccessDelegation {

  /**
   * The property that turns on source-catalog vending for a Delta/Unity connector.
   *
   * <p>Set with {@code --props databricks.access-delegation=vended-credentials}. Honored only for
   * the Delta-family connector kinds; a stray copy on an Iceberg connector is ignored, since that
   * path is governed by {@link IcebergAccessDelegation#HEADER_PROPERTY} instead.
   *
   * <p>The key ends in {@code access-delegation} rather than {@code vend-credentials} on purpose:
   * the connector-property secret guard rejects any key whose canonical form ends in {@code
   * _credentials} (to keep plaintext secrets out of connector metadata), and this is a non-secret
   * boolean opt-in. The name also mirrors the Iceberg gate's {@code X-Iceberg-Access-Delegation}
   * header, whose {@code vended-credentials} value {@link #isTruthy} already accepts.
   */
  public static final String VEND_OPTION = "databricks.access-delegation";

  private static final String DELTA_SOURCE_OPTION = "delta.source";

  private DatabricksAccessDelegation() {}

  /**
   * Whether {@code config} is a Delta/Unity connector that has opted in to catalog vending.
   *
   * <p>Answerable from {@link ConnectorConfig} alone because both gates run before the connector
   * factory builds the connector, exactly as the Iceberg gate requires.
   */
  public static boolean declaresVendedCredentials(ConnectorConfig config) {
    if (config == null) {
      return false;
    }
    if (!usesUnityCatalog(config)) {
      return false;
    }
    return isTruthy(config.options().get(VEND_OPTION));
  }

  private static boolean usesUnityCatalog(ConnectorConfig config) {
    if (config.kind() == ConnectorConfig.Kind.UNITY) {
      return true;
    }
    if (config.kind() != ConnectorConfig.Kind.DELTA) {
      return false;
    }
    String source = config.options().get(DELTA_SOURCE_OPTION);
    return source == null || source.isBlank() || source.trim().equalsIgnoreCase("unity");
  }

  private static boolean isTruthy(String value) {
    if (value == null || value.isBlank()) {
      return false;
    }
    String token = value.trim().toLowerCase(Locale.ROOT);
    return token.equals("true")
        || token.equals("1")
        || token.equals("yes")
        || token.equals("vended-credentials");
  }
}

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
import java.util.Set;

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
   * <p>Set with {@code --props databricks.access-delegation=vended-credentials}. Honored only for a
   * Delta connector whose {@code delta.source} is Unity Catalog; a stray copy on an Iceberg
   * connector is ignored, since that path is governed by {@link
   * IcebergAccessDelegation#HEADER_PROPERTY} instead.
   *
   * <p>The key ends in {@code access-delegation} rather than {@code vend-credentials} on purpose:
   * the connector-property secret guard rejects any key whose canonical form ends in {@code
   * _credentials} (to keep plaintext secrets out of connector metadata), and this is a non-secret
   * boolean opt-in. The name also mirrors the Iceberg gate's {@code X-Iceberg-Access-Delegation}
   * header, whose {@code vended-credentials} value {@link #isTruthy} already accepts.
   */
  public static final String VEND_OPTION = "databricks.access-delegation";

  private static final String DELTA_SOURCE_OPTION = "delta.source";

  private static final Set<String> TRUTHY = Set.of("true", "1", "yes", "vended-credentials");

  private static final Set<String> FALSY = Set.of("false", "0", "no", "none");

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

  /**
   * Whether this Delta connector is pointed at Unity Catalog.
   *
   * <p>The catalog, not the format, is what can vend: {@code delta.source=glue} has no equivalent
   * API and {@code delta.source=filesystem} has no catalog at all, so both keep using a storage
   * authority.
   *
   * <p>An absent {@code delta.source} still reads as Unity Catalog because that has always been
   * {@code DeltaConnectorFactory.selectSource}'s default, and connectors persisted before the
   * option was required rely on it. New connectors must state it explicitly -- the service rejects
   * a Delta spec without it -- so this fallback only ever applies to legacy configuration.
   */
  private static boolean usesUnityCatalog(ConnectorConfig config) {
    if (config.kind() != ConnectorConfig.Kind.DELTA) {
      return false;
    }
    String source = config.options().get(DELTA_SOURCE_OPTION);
    return source == null || source.isBlank() || source.trim().equalsIgnoreCase("unity");
  }

  /**
   * Whether a value for {@link #VEND_OPTION} means anything to this parser.
   *
   * <p>Exists so the service can reject a typo at create/update time. {@link #isTruthy} answers
   * {@code false} for everything it does not recognise, which is the only safe reading at request
   * time but makes {@code vended_credentials} (underscore) or {@code vended-credential} (singular)
   * indistinguishable from a deliberate opt-out -- and because the "attempt vending" gate and the
   * reconciler's "absorb the missing-authority error" gate both read this parser, the two agree
   * that the connector never opted in and nothing anywhere reports why reads fall back to a storage
   * authority that was never configured.
   *
   * <p>A blank value is recognised: an absent or cleared property is simply "not opted in".
   */
  public static boolean isRecognizedValue(String value) {
    if (value == null || value.isBlank()) {
      return true;
    }
    return isTruthy(value) || FALSY.contains(value.trim().toLowerCase(Locale.ROOT));
  }

  private static boolean isTruthy(String value) {
    if (value == null || value.isBlank()) {
      return false;
    }
    return TRUTHY.contains(value.trim().toLowerCase(Locale.ROOT));
  }
}

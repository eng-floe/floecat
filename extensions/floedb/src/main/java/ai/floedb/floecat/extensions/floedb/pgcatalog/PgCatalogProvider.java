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

package ai.floedb.floecat.extensions.floedb.pgcatalog;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.extensions.floedb.engine.FloeTypeMapper;
import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.metagraph.model.TableBackendKind;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import ai.floedb.floecat.systemcatalog.util.EngineContextNormalizer;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * pg_catalog system catalog provider for the FloeDB engine.
 *
 * <p>This provider exposes a minimal PostgreSQL-compatible pg_catalog schema by projecting Metacat
 * metadata into well-known pg_* tables. The intent is driver compatibility (JDBC, SQLAlchemy, BI
 * tools), not full PostgreSQL emulation.
 *
 * <p>All objects are derived at runtime via scanners; no pg_catalog objects are loaded from static
 * builtin definitions.
 */
public final class PgCatalogProvider implements SystemObjectScannerProvider {

  public static final int PG_CATALOG_OID = 11;
  private static final Set<String> SUPPORTED_ENGINES = Set.of("floedb", "floe-demo");

  private final Map<String, SystemObjectScanner> scanners =
      Map.of(
          "pg_namespace_scanner", new PgNamespaceScanner(),
          "pg_class_scanner", new PgClassScanner(),
          "pg_attribute_scanner", new PgAttributeScanner(new FloeTypeMapper()),
          "pg_type_scanner", new PgTypeScanner(),
          "pg_proc_scanner", new PgProcScanner());

  @Override
  public List<SystemObjectDef> definitions() {
    return List.of(
        // pg_catalog namespace
        new SystemNamespaceDef(
            NameRefUtil.name("pg_catalog"),
            "pg_catalog",
            List.of(pgCatalogRule("floedb"), pgCatalogRule("floe-demo"))),

        // pg_namespace
        new SystemTableDef(
            NameRefUtil.name("pg_catalog", "pg_namespace"),
            "pg_namespace",
            PgNamespaceScanner.SCHEMA,
            TableBackendKind.FLOECAT,
            "pg_namespace_scanner",
            List.of()),

        // // pg_class
        new SystemTableDef(
            NameRefUtil.name("pg_catalog", "pg_class"),
            "pg_class",
            PgClassScanner.SCHEMA,
            TableBackendKind.FLOECAT,
            "pg_class_scanner",
            List.of()),

        // // pg_attribute
        new SystemTableDef(
            NameRefUtil.name("pg_catalog", "pg_attribute"),
            "pg_attribute",
            PgAttributeScanner.SCHEMA,
            TableBackendKind.FLOECAT,
            "pg_attribute_scanner",
            List.of()),

        // pg_type
        new SystemTableDef(
            NameRefUtil.name("pg_catalog", "pg_type"),
            "pg_type",
            PgTypeScanner.SCHEMA,
            TableBackendKind.FLOECAT,
            "pg_type_scanner",
            List.of()),

        // pg_proc
        new SystemTableDef(
            NameRefUtil.name("pg_catalog", "pg_proc"),
            "pg_proc",
            PgProcScanner.SCHEMA,
            TableBackendKind.FLOECAT,
            "pg_proc_scanner",
            List.of()));
  }

  @Override
  public boolean supportsEngine(String engineKind) {
    String normalized = EngineContextNormalizer.normalizeEngineKind(engineKind);
    return SUPPORTED_ENGINES.contains(normalized);
  }

  @Override
  public boolean supports(NameRef name, String engineKind) {
    if (!supportsEngine(engineKind) || name == null) {
      return false;
    }

    // Objects must live directly under pg_catalog
    if (name.getPathCount() != 1) {
      return false;
    }

    return "pg_catalog".equalsIgnoreCase(name.getPath(0));
  }

  @Override
  public Optional<SystemObjectScanner> provide(
      String scannerId, String engineKind, String engineVersion) {

    if (!supportsEngine(engineKind) || scannerId == null) {
      return Optional.empty();
    }

    return Optional.ofNullable(scanners.get(scannerId.toLowerCase()));
  }

  private static EngineSpecificRule pgCatalogRule(String engineKind) {
    return new EngineSpecificRule(
        engineKind,
        "",
        "",
        "floe.namespace+proto",
        FloeNamespaceSpecific.newBuilder()
            .setOid(PG_CATALOG_OID)
            .setNspname("pg_catalog")
            .build()
            .toByteArray(),
        Map.of());
  }
}

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

package ai.floedb.floecat.systemcatalog.testsupport;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.TableBackendKind;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.scanner.utils.EnvironmentContext;
import ai.floedb.floecat.systemcatalog.def.SystemColumnDef;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.provider.CatalogEnvironmentProvider;
import ai.floedb.floecat.systemcatalog.spi.EngineCatalogProvider;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** Shared test providers for SystemNodeRegistry-related tests. */
public final class SystemCatalogTestProviders {

  private SystemCatalogTestProviders() {}

  public static final class VersionedTableProvider implements EngineCatalogProvider {

    private final String engineKind;
    private final AtomicInteger definitionsCalled = new AtomicInteger();

    public VersionedTableProvider(String engineKind) {
      this.engineKind = engineKind;
    }

    @Override
    public String engineKind() {
      return engineKind;
    }

    @Override
    public List<SystemObjectDef> definitions(CatalogContext context) {
      definitionsCalled.incrementAndGet();
      return List.of(
          namespaceFor(context.engine().normalizedKind()),
          tableFor(context.engine().normalizedKind(), context.engine().normalizedVersion()));
    }

    @Override
    public Optional<SystemObjectScanner> provide(String scannerId, CatalogContext context) {
      return Optional.empty();
    }

    public int invocationCount() {
      return definitionsCalled.get();
    }

    private SystemTableDef tableFor(String engineKind, String engineVersion) {
      String suffix = engineVersion == null || engineVersion.isEmpty() ? "default" : engineVersion;
      return new SystemTableDef(
          NameRefUtil.name(engineKind, "versioned_" + suffix),
          "versioned_" + suffix,
          List.<SystemColumnDef>of(),
          TableBackendKind.TABLE_BACKEND_KIND_ENGINE,
          "",
          "",
          "",
          List.of(),
          null);
    }

    private SystemNamespaceDef namespaceFor(String engineKind) {
      return new SystemNamespaceDef(NameRefUtil.name(engineKind), engineKind, List.of());
    }
  }

  public static final class EnvironmentTableProvider implements CatalogEnvironmentProvider {

    private final String environmentKind;
    private final String tableName;

    public EnvironmentTableProvider(String environmentKind, String tableName) {
      this.environmentKind = environmentKind;
      this.tableName = tableName;
    }

    @Override
    public String environmentKind() {
      return environmentKind;
    }

    @Override
    public List<SystemObjectDef> definitions(CatalogContext context) {
      return List.of(
          new SystemNamespaceDef(NameRefUtil.name("environment"), "environment", List.of()),
          new SystemTableDef(
              NameRefUtil.name("environment", tableName),
              tableName,
              List.of(),
              TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
              "environment-scanner",
              "",
              "",
              List.of(),
              null));
    }

    @Override
    public boolean supportsEnvironment(EnvironmentContext environment) {
      return CatalogEnvironmentProvider.super.supportsEnvironment(environment);
    }

    @Override
    public boolean supports(NameRef name, CatalogContext context) {
      return true;
    }

    @Override
    public Optional<SystemObjectScanner> provide(String scannerId, CatalogContext context) {
      return Optional.empty();
    }
  }

  public static final class EngineTableProvider implements EngineCatalogProvider {

    private final String engineKind;
    private final NameRef name;

    public EngineTableProvider(String engineKind, NameRef name) {
      this.engineKind = engineKind;
      this.name = name;
    }

    @Override
    public String engineKind() {
      return engineKind;
    }

    @Override
    public List<SystemObjectDef> definitions(CatalogContext context) {
      NameRef namespace = NameRefUtil.namespaceRef(name).orElseThrow();
      return List.of(
          new SystemNamespaceDef(namespace, NameRefUtil.canonical(namespace), List.of()),
          tableDef());
    }

    @Override
    public Optional<SystemObjectScanner> provide(String scannerId, CatalogContext context) {
      return Optional.empty();
    }

    private SystemTableDef tableDef() {
      return new SystemTableDef(
          name,
          "overridden",
          List.<SystemColumnDef>of(),
          TableBackendKind.TABLE_BACKEND_KIND_ENGINE,
          "",
          "",
          "",
          List.of(),
          null);
    }
  }
}

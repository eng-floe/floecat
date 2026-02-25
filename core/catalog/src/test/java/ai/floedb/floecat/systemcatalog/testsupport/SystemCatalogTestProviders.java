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
import ai.floedb.floecat.systemcatalog.def.SystemColumnDef;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** Shared test providers for SystemNodeRegistry-related tests. */
public final class SystemCatalogTestProviders {

  private SystemCatalogTestProviders() {}

  public static final class VersionedTableProvider implements SystemObjectScannerProvider {

    private final String engineKind;
    private final AtomicInteger definitionsCalled = new AtomicInteger();

    public VersionedTableProvider(String engineKind) {
      this.engineKind = engineKind;
    }

    @Override
    public List<SystemObjectDef> definitions() {
      return definitions(engineKind, "");
    }

    @Override
    public List<SystemObjectDef> definitions(String engineKind, String engineVersion) {
      definitionsCalled.incrementAndGet();
      return List.of(tableFor(engineKind, engineVersion));
    }

    @Override
    public boolean supportsEngine(String engineKind) {
      return this.engineKind.equals(engineKind);
    }

    @Override
    public boolean supports(NameRef name, String engineKind) {
      return this.engineKind.equals(engineKind);
    }

    @Override
    public boolean supports(NameRef name, String engineKind, String engineVersion) {
      return supports(name, engineKind);
    }

    @Override
    public Optional<SystemObjectScanner> provide(
        String scannerId, String engineKind, String engineVersion) {
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
          TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
          "version-scanner",
          "",
          "",
          List.of(),
          null);
    }
  }

  public static final class OverridingTableProvider implements SystemObjectScannerProvider {

    private final String engineKind;
    private final NameRef name;
    private final String scannerId;

    public OverridingTableProvider(String engineKind, NameRef name, String scannerId) {
      this.engineKind = engineKind;
      this.name = name;
      this.scannerId = scannerId;
    }

    @Override
    public List<SystemObjectDef> definitions() {
      return List.of(tableDef());
    }

    @Override
    public boolean supportsEngine(String engineKind) {
      return this.engineKind.equals(engineKind);
    }

    @Override
    public boolean supports(NameRef name, String engineKind) {
      return this.engineKind.equals(engineKind);
    }

    @Override
    public boolean supports(NameRef name, String engineKind, String engineVersion) {
      return supports(name, engineKind);
    }

    @Override
    public Optional<SystemObjectScanner> provide(
        String scannerId, String engineKind, String engineVersion) {
      return Optional.empty();
    }

    private SystemTableDef tableDef() {
      return new SystemTableDef(
          name,
          "overridden",
          List.<SystemColumnDef>of(),
          TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
          scannerId,
          "",
          "",
          List.of(),
          null);
    }
  }

  public static final class RegistryHintProvider implements SystemObjectScannerProvider {

    private final String engineKind;
    private final List<EngineSpecificRule> hints;

    public RegistryHintProvider(String engineKind, List<EngineSpecificRule> hints) {
      this.engineKind = engineKind;
      this.hints = List.copyOf(hints);
    }

    @Override
    public List<SystemObjectDef> definitions() {
      return List.of();
    }

    @Override
    public boolean supportsEngine(String engineKind) {
      return this.engineKind.equals(engineKind);
    }

    @Override
    public boolean supports(NameRef name, String engineKind) {
      return supportsEngine(engineKind);
    }

    @Override
    public Optional<SystemObjectScanner> provide(
        String scannerId, String engineKind, String engineVersion) {
      return Optional.empty();
    }

    @Override
    public List<EngineSpecificRule> registryEngineSpecific(
        String engineKind, String engineVersion) {
      return hints;
    }
  }
}

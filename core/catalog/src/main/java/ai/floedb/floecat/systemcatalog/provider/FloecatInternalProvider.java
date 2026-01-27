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

package ai.floedb.floecat.systemcatalog.provider;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.SystemObjectsRegistry;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.informationschema.InformationSchemaProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogProtoMapper;
import ai.floedb.floecat.systemcatalog.registry.SystemObjectsRegistryMerger;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import ai.floedb.floecat.systemcatalog.util.EngineCatalogNames;
import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Provider for the floecat_internal catalog layer. */
public final class FloecatInternalProvider implements SystemObjectScannerProvider {

  private static final String RESOURCE_DIR = "/builtins/floecat_internal";
  private static final String INDEX_PATH = RESOURCE_DIR + "/_index.txt";

  private static final SystemCatalogData CATALOG = loadCatalogData();

  private final InformationSchemaProvider informationSchema = new InformationSchemaProvider();

  private final List<SystemObjectDef> definitions =
      List.copyOf(
          Stream.concat(CATALOG.namespaces().stream(), CATALOG.tables().stream())
              .collect(Collectors.toUnmodifiableList()));

  @Override
  public List<SystemObjectDef> definitions() {
    return definitions;
  }

  @Override
  public boolean supportsEngine(String engineKind) {
    return true;
  }

  @Override
  public boolean supports(NameRef name, String engineKind) {
    return informationSchema.supports(name, engineKind);
  }

  @Override
  public Optional<SystemObjectScanner> provide(
      String scannerId, String engineKind, String engineVersion) {
    return informationSchema.provide(scannerId, engineKind, engineVersion);
  }

  private static SystemCatalogData loadCatalogData() {
    List<String> fragments = loadIndex();
    SystemObjectsRegistry.Builder accumulator = SystemObjectsRegistry.newBuilder();
    TextFormat.Parser parser = TextFormat.Parser.newBuilder().build();
    for (String fragment : fragments) {
      String resourcePath = RESOURCE_DIR + "/" + fragment;
      String raw = readResource(resourcePath);
      SystemObjectsRegistry.Builder tmp = SystemObjectsRegistry.newBuilder();
      try {
        parser.merge(raw, tmp);
      } catch (TextFormat.ParseException e) {
        throw new IllegalStateException(
            "Failed to parse builtin fragment: " + resourcePath + ": " + e.getMessage(), e);
      }
      SystemObjectsRegistryMerger.append(accumulator, tmp);
    }
    SystemObjectsRegistry merged = accumulator.build();
    return SystemCatalogProtoMapper.fromProto(merged, EngineCatalogNames.FLOECAT_DEFAULT_CATALOG);
  }

  private static List<String> loadIndex() {
    String raw = readResource(INDEX_PATH);
    return raw.lines()
        .map(String::trim)
        .filter(line -> !line.isEmpty() && !line.startsWith("#"))
        .collect(Collectors.toUnmodifiableList());
  }

  private static String readResource(String resourcePath) {
    try (InputStream in = FloecatInternalProvider.class.getResourceAsStream(resourcePath)) {
      if (in == null) {
        throw new IllegalStateException("Builtin resource missing: " + resourcePath);
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read builtin resource: " + resourcePath, e);
    }
  }

  public static SystemCatalogData catalogData() {
    return CATALOG;
  }
}

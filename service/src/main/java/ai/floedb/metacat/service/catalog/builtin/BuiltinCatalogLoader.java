package ai.floedb.metacat.service.catalog.builtin;

import ai.floedb.metacat.catalog.rpc.BuiltinCatalog;
import com.google.protobuf.TextFormat;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Loads immutable builtin catalog files (`builtin_catalog_<engine>.pb|pbtxt`) from the configured
 * location, caches them by engine version, and exposes them to service callers.
 */
@ApplicationScoped
public class BuiltinCatalogLoader {

  static final String DEFAULT_LOCATION = "classpath:builtins";

  @ConfigProperty(name = "metacat.builtins.location", defaultValue = DEFAULT_LOCATION)
  String configuredLocation;

  private final Map<String, BuiltinCatalogData> cache = new ConcurrentHashMap<>();

  private String location;

  @PostConstruct
  void init() {
    location = configuredLocation == null ? DEFAULT_LOCATION : configuredLocation.trim();
    if (location.isBlank()) {
      location = DEFAULT_LOCATION;
    }
  }

  public BuiltinCatalogData getCatalog(String engineVersion) {
    if (engineVersion == null || engineVersion.isBlank()) {
      throw new IllegalArgumentException("engine_version must be provided");
    }
    return cache.computeIfAbsent(engineVersion, this::loadCatalog);
  }

  private BuiltinCatalogData loadCatalog(String engineVersion) {
    String base = "builtin_catalog_" + engineVersion;

    try (InputStream binary = openStream(base + ".pb")) {
      if (binary != null) {
        return BuiltinCatalogProtoMapper.fromProto(BuiltinCatalog.parseFrom(binary));
      }
    } catch (IOException e) {
      throw new BuiltinCatalogLoadException(engineVersion, e);
    }

    try (InputStream text = openStream(base + ".pbtxt")) {
      if (text != null) {
        var builder = BuiltinCatalog.newBuilder();
        try (var reader = new InputStreamReader(text, StandardCharsets.UTF_8)) {
          TextFormat.getParser().merge(reader, builder);
        }
        return BuiltinCatalogProtoMapper.fromProto(builder.build());
      }
    } catch (IOException e) {
      throw new BuiltinCatalogLoadException(engineVersion, e);
    }

    throw new BuiltinCatalogNotFoundException(engineVersion);
  }

  private InputStream openStream(String fileName) throws IOException {
    Objects.requireNonNull(fileName, "fileName");

    // Treat `classpath:` specially so we can bundle sample catalogs in the jar.
    if (location.startsWith("classpath:")) {
      String base = location.substring("classpath:".length());
      if (base.startsWith("/")) {
        base = base.substring(1);
      }
      String resource =
          base.isBlank() ? fileName : base.endsWith("/") ? base + fileName : base + "/" + fileName;
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      return cl.getResourceAsStream(resource);
    }

    // Otherwise resolve against a filesystem path (explicit `file:` or bare directory).
    String basePath = location.startsWith("file:") ? location.substring(5) : location;
    Path dir = Paths.get(basePath);
    Path resolved = dir.resolve(fileName);
    if (Files.exists(resolved)) {
      return Files.newInputStream(resolved);
    }
    return null;
  }
}

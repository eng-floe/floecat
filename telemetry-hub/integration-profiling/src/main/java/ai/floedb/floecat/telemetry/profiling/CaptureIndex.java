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

package ai.floedb.floecat.telemetry.profiling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class CaptureIndex {
  private static final Duration ORPHAN_DELETE_DELAY = Duration.ofMinutes(5);
  private static final Logger LOG = LoggerFactory.getLogger(CaptureIndex.class);
  private static final Pattern CAPTURE_ID_PATTERN = Pattern.compile("^[A-Za-z0-9-]{1,64}$");
  private final ProfilingConfig config;
  private final Path artifactDir;
  private final ObjectMapper mapper;
  private final Map<String, CaptureMetadata> captures = new ConcurrentHashMap<>();

  @Inject
  public CaptureIndex(ProfilingConfig config) {
    this.config = config;
    this.artifactDir = Path.of(config.artifactDir()).toAbsolutePath().normalize();
    this.mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
  }

  @PostConstruct
  void init() throws IOException {
    Files.createDirectories(artifactDir);
    Map<String, Path> jsonFiles = new ConcurrentHashMap<>();
    try (Stream<Path> files = Files.list(artifactDir)) {
      files
          .filter(path -> path.toString().endsWith(".json"))
          .forEach(
              path -> {
                try {
                  CaptureMetadata meta = mapper.readValue(path.toFile(), CaptureMetadata.class);
                  captures.put(meta.getId(), meta);
                  jsonFiles.put(meta.getId(), path);
                } catch (IOException e) {
                  LOG.warn("failed to read profiling metadata {}", path, e);
                }
              });
    }
    Instant now = Instant.now();
    Instant cutoff = now.minus(ORPHAN_DELETE_DELAY);
    try (Stream<Path> files = Files.list(artifactDir)) {
      files
          .filter(path -> path.toString().endsWith(".jfr"))
          .forEach(
              path -> {
                String id = path.getFileName().toString();
                if (id.endsWith(".jfr")) {
                  id = id.substring(0, id.length() - 4);
                }
                if (id.isBlank() || captures.containsKey(id)) {
                  return;
                }
                try {
                  FileTime modified = Files.getLastModifiedTime(path);
                  if (!modified.toInstant().isBefore(cutoff)) {
                    LOG.info(
                        "preserving recent orphan profiling artifact {} (age {}s)",
                        path,
                        Duration.between(modified.toInstant(), now).getSeconds());
                    return;
                  }
                } catch (IOException e) {
                  LOG.warn("unable to inspect orphan profiling artifact {}", path, e);
                }
                LOG.info("removing orphan profiling artifact {}", path);
                deleteFile(path);
                jsonFiles.remove(id);
              });
    }
    for (Map.Entry<String, Path> entry : jsonFiles.entrySet()) {
      String id = entry.getKey();
      Path jsonPath = entry.getValue();
      Path artifact = artifactDir.resolve(id + ".jfr");
      if (Files.exists(artifact)) {
        continue;
      }
      try {
        FileTime modified = Files.getLastModifiedTime(jsonPath);
        if (modified.toInstant().isBefore(cutoff)) {
          LOG.info("removing orphan profiling metadata {}", jsonPath);
          deleteFile(jsonPath);
          captures.remove(id);
        }
      } catch (IOException e) {
        LOG.warn("unable to inspect orphan profiling metadata {}", jsonPath, e);
      }
    }
  }

  Path artifactFor(String id) {
    return resolveCaptureFile(id, ".jfr");
  }

  void persist(CaptureMetadata meta) {
    captures.put(meta.getId(), meta);
    ensureArtifactSize(meta);
    Path metadataPath = resolveCaptureFile(meta.getId(), ".json");
    try {
      mapper.writeValue(metadataPath.toFile(), meta);
    } catch (IOException e) {
      // best effort
    }
  }

  Optional<CaptureMetadata> latest() {
    return captures.values().stream()
        .filter(m -> m.getStartTime() != null)
        .max(Comparator.comparing(CaptureMetadata::getStartTime));
  }

  Optional<CaptureMetadata> find(String id) {
    return Optional.ofNullable(captures.get(id));
  }

  long totalBytes() {
    return captures.values().stream()
        .mapToLong(
            meta -> {
              Long bytes = meta.getArtifactSizeBytes();
              if (bytes == null) {
                ensureArtifactSize(meta);
                bytes = meta.getArtifactSizeBytes();
              }
              return bytes == null ? 0L : bytes;
            })
        .sum();
  }

  Stream<CaptureMetadata> all() {
    return captures.values().stream();
  }

  void pruneTo(long maxBytes) throws IOException {
    while (totalBytes() > maxBytes) {
      Optional<CaptureMetadata> oldest =
          captures.values().stream()
              .filter(meta -> meta.getStartTime() != null)
              .min(Comparator.comparing(CaptureMetadata::getStartTime));
      if (oldest.isEmpty()) {
        break;
      }
      removeOldest(oldest.get());
    }
  }

  private void removeOldest(CaptureMetadata meta) {
    captures.remove(meta.getId());
    deleteFile(resolveCaptureFile(meta.getId(), ".jfr"));
    deleteFile(resolveCaptureFile(meta.getId(), ".json"));
  }

  private static void deleteFile(Path path) {
    if (path == null) {
      return;
    }
    try {
      Files.deleteIfExists(path);
    } catch (IOException ignored) {
    }
  }

  private void ensureArtifactSize(CaptureMetadata meta) {
    if (meta.getArtifactSizeBytes() != null) {
      return;
    }
    Path artifact;
    try {
      artifact = resolveCaptureFile(meta.getId(), ".jfr");
    } catch (IllegalArgumentException e) {
      LOG.warn("skipping artifact size for invalid capture id {}", meta.getId());
      return;
    }
    if (Files.exists(artifact)) {
      try {
        meta.setArtifactSizeBytes(Files.size(artifact));
      } catch (IOException ignored) {
      }
    }
  }

  private Path resolveCaptureFile(String id, String extension) {
    validateCaptureId(id);
    Path candidate = artifactDir.resolve(id + extension).normalize();
    if (!candidate.startsWith(artifactDir)) {
      throw new IllegalArgumentException("capture path escapes artifact directory: " + id);
    }
    return candidate;
  }

  private void validateCaptureId(String id) {
    if (id == null || !CAPTURE_ID_PATTERN.matcher(id).matches()) {
      throw new IllegalArgumentException("invalid capture id: " + id);
    }
  }

  @PreDestroy
  void shutdown() {
    // no-op
  }
}

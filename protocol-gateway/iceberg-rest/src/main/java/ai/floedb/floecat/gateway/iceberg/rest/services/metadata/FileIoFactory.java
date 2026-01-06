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

package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.storage.spi.io.RuntimeFileIoOverrides;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.io.FileIO;

public final class FileIoFactory {
  private static final String DEFAULT_IO_IMPL = "org.apache.iceberg.aws.s3.S3FileIO";
  private static final Set<String> IO_PROP_PREFIXES =
      Set.of("s3.", "s3a.", "s3n.", "fs.", "client.", "aws.", "hadoop.");

  private FileIoFactory() {}

  public static FileIO createFileIo(
      Map<String, String> props, IcebergGatewayConfig config, boolean allowConfigOverrides) {
    Map<String, String> normalized =
        props == null ? new LinkedHashMap<>() : new LinkedHashMap<>(props);
    if (allowConfigOverrides && config != null) {
      config.metadataFileIoRoot().ifPresent(root -> normalized.put("fs.floecat.test-root", root));
    }
    RuntimeFileIoOverrides.mergeInto(normalized);
    String impl =
        allowConfigOverrides && config != null
            ? config.metadataFileIo().orElse(normalized.getOrDefault("io-impl", DEFAULT_IO_IMPL))
            : normalized.getOrDefault("io-impl", DEFAULT_IO_IMPL);
    impl = impl == null ? DEFAULT_IO_IMPL : impl.trim();
    try {
      Class<?> clazz = Class.forName(impl);
      Object instance = clazz.getDeclaredConstructor().newInstance();
      if (!(instance instanceof FileIO fileIO)) {
        throw new IllegalArgumentException(impl + " does not implement FileIO");
      }
      fileIO.initialize(filterIoProperties(normalized));
      return fileIO;
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to instantiate FileIO " + impl, e);
    }
  }

  public static Map<String, String> filterIoProperties(Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return Map.of();
    }
    Map<String, String> filtered = new LinkedHashMap<>();
    props.forEach(
        (key, value) -> {
          if (key == null || value == null) {
            return;
          }
          if (isFileIoProperty(key)) {
            filtered.put(key, value);
          }
        });
    return filtered;
  }

  public static boolean isFileIoProperty(String key) {
    if (key == null || key.isBlank()) {
      return false;
    }
    if ("io-impl".equals(key)) {
      return true;
    }
    for (String prefix : IO_PROP_PREFIXES) {
      if (key.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }
}

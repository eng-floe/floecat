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

import ai.floedb.floecat.gateway.iceberg.rest.config.ConnectorIntegrationConfig;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.jboss.logging.Logger;

@ApplicationScoped
public class MetadataFileIoSupport {
  private static final Logger LOG = Logger.getLogger(MetadataFileIoSupport.class);

  @Inject ConnectorIntegrationConfig config;
  @Inject TableGatewaySupport tableGatewaySupport;

  FileIO newImportFileIo(Map<String, String> ioProperties) {
    return FileIoFactory.createFileIo(ioProperties, null, false);
  }

  FileIO newMaterializationFileIo(Map<String, String> metadataProperties) {
    return FileIoFactory.createFileIo(
        materializationIoProperties(metadataProperties), config, true);
  }

  Map<String, String> materializationIoProperties(Map<String, String> metadataProperties) {
    Map<String, String> props = new LinkedHashMap<>();
    if (tableGatewaySupport != null) {
      props.putAll(tableGatewaySupport.defaultFileIoProperties());
    }
    if (metadataProperties != null && !metadataProperties.isEmpty()) {
      metadataProperties.forEach(
          (key, value) -> {
            if (key != null && value != null) {
              props.put(key, value);
            }
          });
    }
    return props;
  }

  void closeQuietly(FileIO fileIO) {
    if (fileIO instanceof AutoCloseable closable) {
      try {
        closable.close();
      } catch (Exception e) {
        LOG.debugf(e, "Failed to close FileIO %s", fileIO.getClass().getName());
      }
    }
  }

  Map<String, String> redactIoProperties(Map<String, String> ioProperties) {
    if (ioProperties == null || ioProperties.isEmpty()) {
      return Map.of();
    }
    Map<String, String> redacted = new LinkedHashMap<>(ioProperties.size());
    ioProperties.forEach(
        (key, value) -> {
          if (key == null) {
            return;
          }
          String normalized = key.toLowerCase();
          if (normalized.contains("secret")
              || normalized.contains("token")
              || normalized.contains("key")) {
            redacted.put(key, "***");
          } else {
            redacted.put(key, value);
          }
        });
    return redacted;
  }
}

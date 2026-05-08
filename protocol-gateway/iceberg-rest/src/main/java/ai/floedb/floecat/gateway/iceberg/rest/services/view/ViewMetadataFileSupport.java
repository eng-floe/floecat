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

package ai.floedb.floecat.gateway.iceberg.rest.services.view;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.ViewMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.config.ConnectorIntegrationConfig;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

@ApplicationScoped
public class ViewMetadataFileSupport {
  @Inject TableGatewaySupport tableGatewaySupport;
  @Inject ObjectMapper mapper;
  @Inject ConnectorIntegrationConfig config;

  public ViewMetadataView loadMetadata(String metadataLocation) {
    FileIO fileIO = null;
    try {
      fileIO =
          FileIoFactory.createFileIo(
              tableGatewaySupport.serverSideFileIoPropertiesForLocation(metadataLocation),
              config,
              true);
      InputFile input = fileIO.newInputFile(metadataLocation);
      try (InputStream stream = input.newStream()) {
        String payload = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        return mapper.readValue(payload, ViewMetadataView.class);
      }
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Unable to read view metadata from " + metadataLocation, e);
    } finally {
      closeQuietly(fileIO);
    }
  }

  private void closeQuietly(FileIO fileIO) {
    if (fileIO instanceof AutoCloseable closable) {
      try {
        closable.close();
      } catch (Exception ignored) {
        // ignore close failure
      }
    }
  }
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.gateway.iceberg.rest.config.ConnectorIntegrationConfig;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class FileIoFactoryTest {

  @Test
  void createFileIoPreservesExplicitEndpoint() {
    Map<String, String> props = new LinkedHashMap<>();
    props.put("io-impl", CaptureFileIo.class.getName());
    props.put("s3.endpoint", "http://localstack:4566");

    ConnectorIntegrationConfig config = Mockito.mock(ConnectorIntegrationConfig.class);
    Mockito.when(config.metadataFileIo()).thenReturn(java.util.Optional.empty());
    Mockito.when(config.metadataFileIoRoot()).thenReturn(java.util.Optional.empty());

    FileIO fileIo = FileIoFactory.createFileIo(props, config, true);

    assertEquals(CaptureFileIo.class, fileIo.getClass());
    assertEquals("http://localstack:4566", CaptureFileIo.lastInitializedProps.get("s3.endpoint"));
  }

  public static final class CaptureFileIo implements FileIO {
    static Map<String, String> lastInitializedProps = Map.of();

    @Override
    public void initialize(Map<String, String> properties) {
      lastInitializedProps = properties == null ? Map.of() : Map.copyOf(properties);
    }

    @Override
    public org.apache.iceberg.io.InputFile newInputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.iceberg.io.OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException();
    }
  }
}

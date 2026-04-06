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

package ai.floedb.floecat.connector.iceberg.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class IcebergConnectorFactoryTest {

  @Test
  void createsRestConnectorWhenSourceRest() {
    var source = IcebergConnectorFactory.selectSource(Map.of("iceberg.source", "rest"));
    assertEquals(IcebergConnectorFactory.IcebergSource.REST, source);
  }

  @Test
  void createsGlueConnectorWhenSourceGlue() {
    var source = IcebergConnectorFactory.selectSource(Map.of("iceberg.source", "glue"));
    assertEquals(IcebergConnectorFactory.IcebergSource.GLUE, source);
  }

  @Test
  void filesystemSourceRequiresUri() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IcebergConnectorFactory.validateOptions(
                    IcebergConnectorFactory.IcebergSource.FILESYSTEM, "", Map.of()));
    assertTrue(ex.getMessage().contains("uri"));
  }

  @Test
  void buildRestPropsMirrorsS3RegionIntoClientRegion() throws Exception {
    var method =
        IcebergConnectorFactory.class.getDeclaredMethod("buildRestProps", String.class, Map.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, String> props =
        (Map<String, String>)
            method.invoke(
                null,
                "https://glue.us-east-2.amazonaws.com/iceberg/",
                Map.of("iceberg.source", "glue", "s3.region", "us-east-2"));

    assertEquals("us-east-2", props.get("s3.region"));
    assertEquals("us-east-2", props.get("client.region"));
  }

  @Test
  void buildRestPropsPreservesExplicitClientRegion() throws Exception {
    var method =
        IcebergConnectorFactory.class.getDeclaredMethod("buildRestProps", String.class, Map.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, String> props =
        (Map<String, String>)
            method.invoke(
                null,
                "https://glue.us-east-2.amazonaws.com/iceberg/",
                Map.of(
                    "iceberg.source", "glue",
                    "s3.region", "us-east-2",
                    "client.region", "us-west-2"));

    assertEquals("us-east-2", props.get("s3.region"));
    assertEquals("us-west-2", props.get("client.region"));
  }
}

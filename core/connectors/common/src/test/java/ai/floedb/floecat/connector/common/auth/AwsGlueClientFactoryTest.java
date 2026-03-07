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

package ai.floedb.floecat.connector.common.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.junit.jupiter.api.Test;

class AwsGlueClientFactoryTest {

  @Test
  void resolveRegionPrefersClientRegion() {
    String region =
        AwsGlueClientFactory.resolveRegion(
            Map.of("client.region", "us-west-2", "s3.region", "us-east-1"), "us-east-1");
    assertEquals("us-west-2", region);
  }

  @Test
  void resolveRegionFallsBackToS3Region() {
    String region =
        AwsGlueClientFactory.resolveRegion(Map.of("s3.region", "eu-west-1"), "us-east-1");
    assertEquals("eu-west-1", region);
  }

  @Test
  void resolveRegionFallsBackToAwsRegion() {
    String region =
        AwsGlueClientFactory.resolveRegion(Map.of("aws.region", "ap-southeast-1"), "us-east-1");
    assertEquals("ap-southeast-1", region);
  }

  @Test
  void resolveRegionUsesDefaultWhenMissing() {
    String region = AwsGlueClientFactory.resolveRegion(Map.of(), "us-east-1");
    assertEquals("us-east-1", region);
  }
}

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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

class AwsProfileSupportTest {

  @Test
  void applyProfilePropertiesCopiesValues() {
    Map<String, String> target = new HashMap<>();
    Map<String, String> authProps =
        Map.of("aws.profile", "dev", "aws.profile_path", "/tmp/aws/config");

    AwsProfileSupport.applyProfileProperties(target, authProps);

    assertEquals("dev", target.get("aws.profile"));
    assertEquals("/tmp/aws/config", target.get("aws.profile_path"));
  }

  @Test
  void resolveProfileProviderRequiresProfileName() {
    assertTrue(AwsProfileSupport.resolveProfileProvider(Map.of()).isEmpty());

    var providerOpt = AwsProfileSupport.resolveProfileProvider(Map.of("aws.profile", "dev"));

    assertTrue(providerOpt.isPresent());
    assertTrue(providerOpt.get() instanceof ProfileCredentialsProvider);
  }

  @Test
  void resolveProfileProviderAcceptsProfilePath() throws Exception {
    var temp = Files.createTempFile("aws-profile", ".credentials");
    temp.toFile().deleteOnExit();

    var providerOpt =
        AwsProfileSupport.resolveProfileProvider(
            Map.of("aws.profile", "dev", "aws.profile_path", temp.toString()));

    assertFalse(providerOpt.isEmpty());
  }
}

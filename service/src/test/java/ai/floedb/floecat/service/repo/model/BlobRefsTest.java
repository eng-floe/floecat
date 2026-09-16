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

package ai.floedb.floecat.service.repo.model;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.MutationMeta;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import org.junit.jupiter.api.Test;

class BlobRefsTest {

  @Test
  void derivesTheStoreEtagFromAContentAddressedUri() throws Exception {
    byte[] digest =
        MessageDigest.getInstance("SHA-256").digest("body".getBytes(StandardCharsets.UTF_8));
    String hex = toHex(digest);
    String expected = Base64.getEncoder().encodeToString(digest);

    assertThat(BlobRefs.etagFromCasUri("/accounts/a/table/" + hex + ".pb")).contains(expected);
  }

  @Test
  void leavesLegacyAndMalformedUrisToTheHeadFallback() {
    assertThat(BlobRefs.etagFromCasUri("/accounts/a/table/table.pb")).isEmpty();
    assertThat(BlobRefs.etagFromCasUri("/accounts/a/table/ABC.pb")).isEmpty();
    assertThat(BlobRefs.etagFromCasUri("/accounts/a/table/" + "0".repeat(64) + ".pb")).isPresent();
  }

  @Test
  void fillsMissingCasVersionWhenConvertingPointerMetadata() {
    String uri = "/accounts/a/table/" + "0".repeat(64) + ".pb";

    assertThat(BlobRefs.refFrom(MutationMeta.newBuilder().setBlobUri(uri).build()).getVersion())
        .isEqualTo(Base64.getEncoder().encodeToString(new byte[32]));
  }

  private static String toHex(byte[] bytes) {
    StringBuilder result = new StringBuilder(bytes.length * 2);
    for (byte value : bytes) {
      result.append(String.format("%02x", value));
    }
    return result.toString();
  }
}

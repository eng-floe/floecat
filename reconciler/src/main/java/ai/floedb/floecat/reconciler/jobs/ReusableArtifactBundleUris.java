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

package ai.floedb.floecat.reconciler.jobs;

import ai.floedb.floecat.types.Hashing;
import java.util.HexFormat;

/** Content-addressing rules for compact reusable artifact bundles. */
public final class ReusableArtifactBundleUris {
  public static final String BUNDLE_DIRECTORY = "reuse-bundles/";
  private static final String BUNDLE_PATH = "/" + BUNDLE_DIRECTORY;

  private ReusableArtifactBundleUris() {}

  public static boolean isBundleUri(String uri) {
    return uri != null && uri.contains(BUNDLE_PATH);
  }

  public static boolean matchesDigest(String uri, byte[] payloadSha256) {
    return uri != null
        && payloadSha256 != null
        && payloadSha256.length == 32
        && uri.endsWith(BUNDLE_PATH + HexFormat.of().formatHex(payloadSha256) + ".pb");
  }

  public static boolean matchesDigest(String uri, String prefix, byte[] payloadSha256) {
    if (uri == null || prefix == null || payloadSha256 == null || payloadSha256.length != 32) {
      return false;
    }
    String separator = prefix.endsWith("/") ? "" : "/";
    return uri.equals(
        prefix
            + separator
            + BUNDLE_DIRECTORY
            + HexFormat.of().formatHex(payloadSha256)
            + ".pb");
  }

  public static boolean matchesPayload(String uri, byte[] payload) {
    return payload != null
        && matchesDigest(uri, HexFormat.of().parseHex(Hashing.sha256Hex(payload)));
  }
}

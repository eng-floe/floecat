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

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.common.rpc.MutationMeta;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Optional;

/** Conversions between pointer metadata and the immutable blob refs the table root stores. */
public final class BlobRefs {

  private BlobRefs() {}

  /**
   * The (uri, etag) ref of the blob a pointer names, or {@code null} when nothing is resolvable.
   */
  public static BlobRef refFrom(MutationMeta meta) {
    if (meta == null || meta.getBlobUri().isEmpty()) {
      return null;
    }
    String version =
        meta.getEtag().isBlank() ? etagFromCasUri(meta.getBlobUri()).orElse("") : meta.getEtag();
    return BlobRef.newBuilder().setUri(meta.getBlobUri()).setVersion(version).build();
  }

  /**
   * Returns the store ETag for a content-addressed URI when the URI carries a SHA-256 filename.
   *
   * <p>Floecat stores the digest in URI paths as lowercase hex, while blob stores expose the same
   * digest as Base64 metadata. Stable URIs therefore let callers avoid a HEAD without changing the
   * externally visible ETag value. Non-CAS and legacy URIs deliberately return empty so their
   * existing HEAD-based behavior remains in place.
   */
  public static Optional<String> etagFromCasUri(String uri) {
    if (uri == null || uri.isBlank()) {
      return Optional.empty();
    }
    int slash = uri.lastIndexOf('/');
    String filename = slash < 0 ? uri : uri.substring(slash + 1);
    int dot = filename.lastIndexOf('.');
    if (dot <= 0) {
      return Optional.empty();
    }
    String hex = filename.substring(0, dot);
    if (hex.length() != 64 || !isLowerHex(hex)) {
      return Optional.empty();
    }
    try {
      return Optional.of(Base64.getEncoder().encodeToString(HexFormat.of().parseHex(hex)));
    } catch (IllegalArgumentException invalidHex) {
      return Optional.empty();
    }
  }

  private static boolean isLowerHex(String value) {
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
        return false;
      }
    }
    return true;
  }
}

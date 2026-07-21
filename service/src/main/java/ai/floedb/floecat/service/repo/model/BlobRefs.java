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
    return BlobRef.newBuilder().setUri(meta.getBlobUri()).setVersion(meta.getEtag()).build();
  }
}

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
package ai.floedb.floecat.service.concurrent;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.function.Supplier;

/**
 * The admitted cache-miss seam for immutable metadata blobs.
 *
 * <p>A content-cache probe is local work, and Caffeine single-flights a cold URI before this seam
 * is reached. Admitting only {@link #load} therefore bounds the one backend read, without making
 * cache hits or followers consume a process-wide metadata-I/O permit.
 */
@ApplicationScoped
public class MetadataIoCacheMissAdmission {

  @BoundMetadataIo
  public <T> T load(Supplier<T> loader) {
    return loader.get();
  }
}

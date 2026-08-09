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

import ai.floedb.floecat.service.repo.util.MetadataReadPolicy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.function.Supplier;

/**
 * Explicit execution policy for blocking metadata repository reads.
 *
 * <p>Each read runs under the process-wide admission ceiling and uses the cancellation signal
 * propagated with its request. Cancellation abandons the waiting caller while the permit remains
 * owned until the downstream operation actually returns. A same-runtime nested read reuses its held
 * permit, and operation failures propagate unchanged.
 */
@ApplicationScoped
public class MetadataResourceReader implements MetadataReadPolicy {

  private static final CancellableCallRunner.FailureMessages FAILURES =
      new CancellableCallRunner.FailureMessages(
          "metadata read cancelled", "interrupted while awaiting metadata-I/O admission");

  private final MetadataIoRunner admission;

  @Inject
  public MetadataResourceReader(MetadataIoRunner admission) {
    this.admission = admission;
  }

  /** Execute {@code reader} under metadata admission for its complete downstream lifetime. */
  @Override
  public <T> T read(Supplier<T> reader) {
    return admission.callWithCurrentCancellation(reader, FAILURES);
  }
}

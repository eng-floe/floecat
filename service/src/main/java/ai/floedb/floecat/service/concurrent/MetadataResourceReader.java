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

import ai.floedb.floecat.service.repo.util.RepositoryReads;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.function.Supplier;

/**
 * Admission policy for one metadata-store operation.
 *
 * <p>The current {@link ai.floedb.floecat.service.context.PropagatedContext} supplies cooperative
 * request cancellation when available. Cancellation may abandon the waiting caller, while the
 * admitted task retains its permit until the backend operation returns. Backend runtime failures
 * propagate unchanged.
 */
@ApplicationScoped
public class MetadataResourceReader implements RepositoryReads.ReadPolicy {

  private static final CancellableCallRunner.FailureMessages FAILURES =
      new CancellableCallRunner.FailureMessages(
          "metadata read cancelled", "interrupted while awaiting metadata-I/O admission");

  private final MetadataIoRunner admission;

  @Inject
  public MetadataResourceReader(MetadataIoRunner admission) {
    this.admission = admission;
  }

  /** Execute one operation under process-wide admission and propagated request cancellation. */
  @Override
  public <T> T read(Supplier<T> operation) {
    return admission.callWithCurrentCancellation(operation, FAILURES);
  }
}

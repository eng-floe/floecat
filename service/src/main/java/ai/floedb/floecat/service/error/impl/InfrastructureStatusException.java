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

package ai.floedb.floecat.service.error.impl;

import ai.floedb.floecat.types.InfrastructureFailure;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * A Floecat status for a condition that ends a call over many relations, such as an expired
 * snapshot or a query that is no longer active, rather than failing one relation.
 */
public final class InfrastructureStatusException extends StatusRuntimeException
    implements InfrastructureFailure {
  InfrastructureStatusException(Status status, Metadata trailers) {
    super(status, trailers);
  }
}

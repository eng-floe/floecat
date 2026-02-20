/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.flight;

import ai.floedb.floecat.flight.context.ResolvedCallContext;
import org.apache.arrow.flight.FlightProducer.CallContext;

/** Resolves the fully materialized {@link ResolvedCallContext} for a Flight RPC. */
public interface FlightCallContextProvider {

  /**
   * Extracts the resolved call context for an inbound Flight call.
   *
   * @param context the Flight call context supplied by Arrow
   * @return the resolved call context, or {@link ResolvedCallContext#unauthenticated()} when no
   *     per-call context is available.
   */
  ResolvedCallContext resolve(CallContext context);
}

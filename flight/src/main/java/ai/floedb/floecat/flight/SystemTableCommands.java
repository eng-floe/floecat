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

package ai.floedb.floecat.flight;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.system.rpc.SystemTableFlightCommand;
import ai.floedb.floecat.system.rpc.SystemTableTarget;
import java.util.Objects;
import org.apache.arrow.flight.FlightDescriptor;

/** Utility helpers for the {@link SystemTableFlightCommand} protocol. */
public final class SystemTableCommands {
  private SystemTableCommands() {}

  public static FlightDescriptor descriptor(String canonicalName, String queryId) {
    return FlightDescriptor.command(command(canonicalName, queryId).toByteArray());
  }

  public static SystemTableFlightCommand command(String canonicalName, String queryId) {
    Objects.requireNonNull(queryId, "queryId");
    if (queryId.isBlank()) {
      throw new IllegalArgumentException("queryId cannot be blank");
    }
    SystemTableTarget target =
        SystemTableTarget.newBuilder()
            .setName(NameRef.newBuilder().setName(canonicalName).build())
            .build();
    return SystemTableFlightCommand.newBuilder().setTarget(target).setQueryId(queryId).build();
  }
}

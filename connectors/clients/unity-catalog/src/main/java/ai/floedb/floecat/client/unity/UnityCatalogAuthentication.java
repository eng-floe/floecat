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

package ai.floedb.floecat.client.unity;

import java.util.Map;
import java.util.Optional;

/** Supplies fresh authentication headers for each Unity Catalog request. */
@FunctionalInterface
public interface UnityCatalogAuthentication {
  Map<String, String> headers();

  /**
   * The secret this puts on the wire, where it is a fixed value worth redacting by name.
   *
   * <p>The generic bearer pattern matches the RFC token68 alphabet, and a header value permits more
   * -- so an operator-supplied token carrying a colon is matched only as far as the colon and the
   * remainder survives into an error message that reaches validation output and operator logs. A
   * client that knows its own secret can redact it exactly, which is what the shared snippet
   * helper's by-value overload is for.
   *
   * <p>Empty by default, and empty for a rotating token: reading one here would mean asking the
   * token provider for a value on a failure path, where a refresh is the last thing wanted. A
   * rotating access token is issued in the token68 shape anyway, so the pattern covers it; the gap
   * is the operator-supplied one.
   */
  default Optional<String> redactableSecret() {
    return Optional.empty();
  }
}

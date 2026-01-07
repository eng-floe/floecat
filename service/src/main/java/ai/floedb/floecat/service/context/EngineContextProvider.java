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

package ai.floedb.floecat.service.context;

import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public final class EngineContextProvider {

  public String engineKind() {
    return InboundContextInterceptor.ENGINE_KIND_KEY.get();
  }

  public String engineVersion() {
    return InboundContextInterceptor.ENGINE_VERSION_KEY.get();
  }

  public boolean isPresent() {
    return engineKind() != null && !engineKind().isBlank();
  }
}

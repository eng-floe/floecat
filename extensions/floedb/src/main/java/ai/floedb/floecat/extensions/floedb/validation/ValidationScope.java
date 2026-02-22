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

package ai.floedb.floecat.extensions.floedb.validation;

import ai.floedb.floecat.scanner.utils.EngineContextNormalizer;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;

public record ValidationScope(String engineKind) {
  public ValidationScope {
    engineKind = EngineContextNormalizer.normalizeEngineKind(engineKind);
  }

  boolean includes(EngineSpecificRule rule) {
    if (rule == null) {
      return false;
    }
    String ruleKind = EngineContextNormalizer.normalizeEngineKind(rule.engineKind());
    if (ruleKind.isEmpty()) {
      return true;
    }
    if (engineKind.isBlank()) {
      return false;
    }
    return engineKind.equals(ruleKind);
  }
}

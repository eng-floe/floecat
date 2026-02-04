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

package ai.floedb.floecat.metagraph.hint;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.jboss.logging.Logger;

/** Simple registry that exposes the currently installed {@link EngineHintPersistence}. */
public final class EngineHintPersistenceRegistry {

  private static final Logger LOG = Logger.getLogger(EngineHintPersistenceRegistry.class);

  private static final AtomicReference<EngineHintPersistence> INSTANCE =
      new AtomicReference<>(EngineHintPersistence.NOOP);

  private EngineHintPersistenceRegistry() {}

  public static void register(EngineHintPersistence persistence) {
    Objects.requireNonNull(persistence, "persistence");
    EngineHintPersistence previous = INSTANCE.getAndSet(persistence);
    if (previous == persistence) {
      return;
    }
    if (previous == EngineHintPersistence.NOOP) {
      return;
    }
    LOG.warnf(
        "Replacing existing EngineHintPersistence (%s) with %s",
        previous.getClass().getName(), persistence.getClass().getName());
  }

  public static EngineHintPersistence get() {
    return INSTANCE.get();
  }
}

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

package ai.floedb.floecat.stats.spi;

import ai.floedb.floecat.catalog.rpc.StatsTarget;

/** Canonical stats target categories understood by routing capabilities. */
public enum StatsTargetType {
  TABLE,
  COLUMN,
  EXPRESSION,
  FILE;

  public static StatsTargetType from(StatsTarget target) {
    return switch (target.getTargetCase()) {
      case TABLE -> TABLE;
      case COLUMN -> COLUMN;
      case EXPRESSION -> EXPRESSION;
      case FILE -> FILE;
      case TARGET_NOT_SET -> throw new IllegalArgumentException("StatsTarget target is not set");
    };
  }
}

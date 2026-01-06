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

package ai.floedb.floecat.metagraph.model;

/**
 * Execution backend for a system object.
 *
 * <p>The enum intentionally mirrors the terminology in {@code FloeCat_Architecture_Book.md}; the
 * concrete RPC/proto definition can replace it once the contract is finalized.
 */
public enum GraphNodeOrigin {
  SYSTEM, // system object nodes (built-in or plugin)
  USER // user objects nodes
}

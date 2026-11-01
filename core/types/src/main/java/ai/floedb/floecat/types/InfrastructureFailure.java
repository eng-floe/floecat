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

package ai.floedb.floecat.types;

/**
 * An exception meaning the system failed, not the relation it was building: storage, a cache or a
 * client being unavailable or shut down, stored data that is lost or not what Floecat wrote, or a
 * control-flow condition that ends the whole call. {@link RelationFailures#isInfrastructure} treats
 * every implementer as infrastructure.
 *
 * <p>A failure of one relation's own definition does not implement it.
 */
public interface InfrastructureFailure {}

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

package ai.floedb.floecat.cache;

/**
 * A value whose retained size {@link CacheWeights} cannot compute.
 *
 * <p>Its own type, rather than a bare {@code IllegalArgumentException}, because of where it is
 * raised: the weigher runs inside the read-through load, so without a type to tell them apart a
 * value type nobody taught the weigher about surfaces as a failed store read. The two need
 * different people -- this one is fixed by implementing {@link WeightedValue} on the value.
 */
public class UnweighableValueException extends IllegalArgumentException {

  private static final long serialVersionUID = 1L;

  public UnweighableValueException(String message) {
    super(message);
  }
}

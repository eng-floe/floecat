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

package ai.floedb.floecat.extensions.floedb.utils;

public final class PayloadDescriptor<T> {

  private final String type;
  private final ThrowingFunction<byte[], T> decoder;

  private PayloadDescriptor(String type, ThrowingFunction<byte[], T> decoder) {
    this.type = type;
    this.decoder = decoder;
  }

  public String type() {
    return type;
  }

  public ThrowingFunction<byte[], T> decoder() {
    return decoder;
  }

  public static <T> PayloadDescriptor<T> of(String type, ThrowingFunction<byte[], T> decoder) {
    return new PayloadDescriptor<>(type, decoder);
  }
}

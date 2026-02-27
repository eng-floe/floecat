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

/** Hex decoding utilities shared by coercion and comparison paths. */
final class HexBytes {
  private HexBytes() {}

  static byte[] decodeHexBytes(String hex) {
    if ((hex.length() & 1) != 0) {
      throw new IllegalArgumentException("Invalid hex BINARY value (odd length): " + hex);
    }
    byte[] out = new byte[hex.length() / 2];
    for (int i = 0; i < out.length; i++) {
      int hi = Character.digit(hex.charAt(2 * i), 16);
      int lo = Character.digit(hex.charAt(2 * i + 1), 16);
      if (hi < 0 || lo < 0) {
        throw new IllegalArgumentException("Invalid hex BINARY value: " + hex);
      }
      out[i] = (byte) ((hi << 4) | lo);
    }
    return out;
  }
}

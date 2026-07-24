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

package ai.floedb.floecat.reconciler.impl;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Locates the message bytes of a repeated length-delimited protobuf field. */
final class ProtobufMessageRanges {
  private ProtobufMessageRanges() {}

  static List<ByteRange> locate(byte[] payload, int fieldNumber) {
    if (payload == null || fieldNumber <= 0) {
      throw new IllegalArgumentException("payload and a positive field number are required");
    }
    CodedInputStream input = CodedInputStream.newInstance(payload);
    List<ByteRange> ranges = new ArrayList<>();
    try {
      while (!input.isAtEnd()) {
        int tag = input.readTag();
        if (tag == 0) {
          break;
        }
        if (WireFormat.getTagFieldNumber(tag) == fieldNumber
            && WireFormat.getTagWireType(tag) == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
          int length = input.readRawVarint32();
          if (length < 0) {
            throw new IllegalArgumentException("negative protobuf message length");
          }
          int offset = input.getTotalBytesRead();
          input.skipRawBytes(length);
          ranges.add(new ByteRange(offset, length));
        } else if (!input.skipField(tag)) {
          break;
        }
      }
      return List.copyOf(ranges);
    } catch (IOException e) {
      throw new IllegalArgumentException("invalid protobuf payload", e);
    }
  }

  record ByteRange(long offset, int length) {}
}

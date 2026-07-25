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
package ai.floedb.floecat.storage.kv;

import java.util.Map;
import java.util.OptionalLong;

/**
 * A typed value of a {@link KvStore.Record} attribute.
 *
 * <p>Attributes carry small bits of metadata alongside a record: pointer targets, TTL stamps, index
 * bookkeeping. They used to be plain strings, which forced numeric metadata to travel as decimal
 * strings and be re-parsed on every read. A {@link NumberValue} is stored natively by backends that
 * have a numeric type (DynamoDB {@code N}), which is what makes atomic server-side increments
 * possible — see {@link KvStore#updateMetadataAttrsIfExists}.
 */
public sealed interface AttrValue permits AttrValue.StringValue, AttrValue.NumberValue {

  /** A string attribute. */
  record StringValue(String value) implements AttrValue {
    public StringValue {
      if (value == null) throw new IllegalArgumentException("string attr value must not be null");
    }
  }

  /** An integral numeric attribute. */
  record NumberValue(long value) implements AttrValue {}

  static AttrValue of(String value) {
    return new StringValue(value);
  }

  static AttrValue of(long value) {
    return new NumberValue(value);
  }

  /** The value rendered as a string; numbers render as their decimal form. */
  default String asString() {
    return switch (this) {
      case StringValue s -> s.value();
      case NumberValue n -> Long.toString(n.value());
    };
  }

  /**
   * A lenient numeric view: a {@link NumberValue} yields its value, a {@link StringValue} yields
   * its parsed decimal form when it has one, and empty otherwise.
   *
   * <p>The leniency is deliberate. Rows written before an attribute was retyped still hold it as a
   * string, so readers of numeric metadata must accept both forms indefinitely.
   */
  default OptionalLong asLong() {
    return switch (this) {
      case NumberValue n -> OptionalLong.of(n.value());
      case StringValue s -> {
        try {
          yield OptionalLong.of(Long.parseLong(s.value()));
        } catch (NumberFormatException e) {
          yield OptionalLong.empty();
        }
      }
    };
  }

  /** Null-safe string read of {@code name} from {@code attrs}, or {@code fallback} if absent. */
  static String stringOr(Map<String, AttrValue> attrs, String name, String fallback) {
    var v = attrs.get(name);
    return v == null ? fallback : v.asString();
  }

  /**
   * Null-safe numeric read of {@code name} from {@code attrs}, or {@code fallback} if the attribute
   * is absent or does not have a numeric form (see {@link #asLong()}).
   */
  static long longOr(Map<String, AttrValue> attrs, String name, long fallback) {
    var v = attrs.get(name);
    return v == null ? fallback : v.asLong().orElse(fallback);
  }
}

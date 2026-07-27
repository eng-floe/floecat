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
   * The value as a number: a {@link NumberValue} yields its value, a {@link StringValue} its parsed
   * decimal form.
   *
   * <p>Accepting both forms is deliberate. Rows written before an attribute was retyped still hold
   * it as a string, so readers of numeric metadata must accept both forms indefinitely.
   *
   * <p>Accepting both forms is not the same as accepting garbage: a present-but-unparsable value is
   * corrupt metadata and throws rather than reading as some default, which for a TTL stamp would
   * quietly make the record immortal.
   *
   * @throws NumberFormatException if the value is a string with no decimal form
   */
  default long asLong() {
    return switch (this) {
      case NumberValue n -> n.value();
      case StringValue s -> Long.parseLong(s.value());
    };
  }

  /** Null-safe string read of {@code name} from {@code attrs}, or {@code fallback} if absent. */
  static String stringOr(Map<String, AttrValue> attrs, String name, String fallback) {
    var v = attrs.get(name);
    return v == null ? fallback : v.asString();
  }

  /**
   * Null-safe numeric read of {@code name} from {@code attrs}, or {@code fallback} if the attribute
   * is absent or unparsable.
   *
   * <p>Only for metadata that degrades gracefully when it is corrupt — cache bookkeeping, where an
   * unreadable counter reading as 0 just makes the entry look cold. Metadata that must not be
   * silently defaulted (a TTL stamp, whose fallback would make the record immortal) reads the
   * attribute directly and lets {@link #asLong()} throw.
   */
  static long longOr(Map<String, AttrValue> attrs, String name, long fallback) {
    var v = attrs.get(name);
    if (v == null) {
      return fallback;
    }
    try {
      return v.asLong();
    } catch (NumberFormatException e) {
      return fallback;
    }
  }
}

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

package ai.floedb.floecat.service.storage;

import java.util.List;

/** Receives one lifecycle for every read crossing the deployed metadata-store interface. */
@FunctionalInterface
public interface StoreReadObserver {
  enum Store {
    POINTER("pointer_store", "pointer"),
    BLOB("blob_store", "blob");

    private final String component;
    private final String summaryName;

    Store(String component, String summaryName) {
      this.component = component;
      this.summaryName = summaryName;
    }

    public String component() {
      return component;
    }

    String summaryName() {
      return summaryName;
    }
  }

  enum Operation {
    GET("get"),
    GET_BATCH("get_batch"),
    GET_RANGE("get_range"),
    GET_RANGES("get_ranges"),
    HEAD("head"),
    LIST("list"),
    LIST_PREFIXES("list_prefixes"),
    SCAN_PREFIX("scan_prefix"),
    SCAN_PREFIX_CONSISTENT("scan_prefix_consistent"),
    COUNT_PREFIX("count_prefix"),
    COUNT_PREFIX_CONSISTENT("count_prefix_consistent"),
    IS_EMPTY("is_empty");

    private final String metricName;

    Operation(String metricName) {
      this.metricName = metricName;
    }

    public String metricName() {
      return metricName;
    }
  }

  /**
   * Immutable call description. Targets exist for local diagnostics only and must never become
   * metric tags or trace attributes.
   */
  record ReadCall(Store store, Operation operation, int itemCount, List<String> targets) {
    public ReadCall {
      if (store == null || operation == null) {
        throw new IllegalArgumentException("store and operation are required");
      }
      if (itemCount < 0) {
        throw new IllegalArgumentException("itemCount must be >= 0");
      }
      targets = targets == null ? List.of() : List.copyOf(targets);
    }

    public String summaryPrefix() {
      return "store_" + store.summaryName() + "_" + operation.metricName();
    }
  }

  /** Whether this observer needs potentially sensitive target names for local diagnostics. */
  default boolean capturesTargets() {
    return false;
  }

  /** Starts observing a call before the underlying store is invoked. */
  Observation begin(ReadCall call);

  interface Observation extends AutoCloseable {
    Observation NOOP = new Observation() {};

    /** Marks a successful call for an operation without a byte unit. */
    default void success() {}

    /** Marks a successful call and records the number of bytes returned. */
    default void success(long bytes) {
      success();
    }

    default void failure(Throwable failure) {}

    @Override
    default void close() {}
  }
}

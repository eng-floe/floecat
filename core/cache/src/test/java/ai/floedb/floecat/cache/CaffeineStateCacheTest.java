/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 */

package ai.floedb.floecat.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class CaffeineStateCacheTest {

  @Test
  void supportsAtomicReplacementAndConditionalInsertion() {
    StateCache<String, Integer> cache =
        CaffeineStateCache.<String, Integer>builder().maximumSize(10).build();

    assertThat(cache.putIfAbsent("key", 1)).isNull();
    assertThat(cache.putIfAbsent("key", 2)).isEqualTo(1);
    assertThat(cache.computeIfPresent("key", (key, value) -> value + 1)).isEqualTo(2);
    assertThat(cache.getIfPresent("key")).isEqualTo(2);
    assertThat(cache.compute("key", (key, value) -> null)).isNull();
    assertThat(cache.getIfPresent("key")).isNull();
  }

  @Test
  void supportsWeightedEntriesAndExpiry() throws Exception {
    StateCache<String, String> cache =
        CaffeineStateCache.<String, String>builder()
            .maximumWeight(2)
            .weigher((key, value) -> value.length())
            .expireAfterWrite(Duration.ofMillis(20))
            .build();

    cache.put("key", "ab");
    assertThat(cache.getIfPresent("key")).isEqualTo("ab");
    Thread.sleep(40);
    assertThat(cache.getIfPresent("key")).isNull();
  }
}

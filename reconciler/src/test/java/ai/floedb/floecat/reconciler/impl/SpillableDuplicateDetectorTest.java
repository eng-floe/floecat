/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexEntry;
import ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SpillableDuplicateDetectorTest {

  @Test
  void detectsDuplicatesAcrossSpillRunsAndFanInMerge() {
    try (SpillableDuplicateDetector detector = new SpillableDuplicateDetector(1L, 2, 1_000_000L)) {
      detector.add("a");
      detector.add("b");
      detector.add("c");
      detector.add("d");
      detector.add("a");

      assertThrows(IllegalArgumentException.class, detector::verifyNoDuplicates);
    }
  }

  @Test
  void replaysEntriesInInsertionOrderAfterSpilling() {
    List<String> expected = List.of("c", "a", "b");
    List<String> actual = new ArrayList<>();
    try (SpillableDuplicateDetector detector = new SpillableDuplicateDetector(1L, 2, 1_000_000L)) {
      for (String value : expected) {
        detector.add(value, entry(value));
      }
      detector.verifyNoDuplicates();
      detector.forEachEntry(entry -> actual.add(entry.getFileStats().getFilePath()));
    }

    assertEquals(expected, actual);
  }

  @Test
  void enforcesLocalDiskBudget() {
    try (SpillableDuplicateDetector detector = new SpillableDuplicateDetector(1L, 2, 1L)) {
      assertThrows(IllegalStateException.class, () -> detector.add("a"));
    }
  }

  private static ReusableArtifactIndexEntry entry(String filePath) {
    return ReusableArtifactIndexEntry.newBuilder()
        .setFileStats(ReusableStatsArtifactMetadata.newBuilder().setFilePath(filePath))
        .build();
  }
}

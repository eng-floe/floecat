/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package ai.floedb.floecat.client.cli;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.jline.reader.impl.history.DefaultHistory;
import org.junit.jupiter.api.Test;

class ShellHistoryTest {

  @Test
  void credentialArgumentsAreExcludedFromPersistentHistory() {
    var history = new TestHistory();

    assertTrue(
        history.matches(
            Shell.SENSITIVE_HISTORY_PATTERN,
            "integration create lakehouse iceberg-rest https://example --cred token=secret"));
    assertTrue(
        history.matches(
            Shell.SENSITIVE_HISTORY_PATTERN,
            "integration update-auth lakehouse --cred secret_access_key=secret"));
    assertFalse(history.matches(Shell.SENSITIVE_HISTORY_PATTERN, "integration get lakehouse"));
  }

  private static final class TestHistory extends DefaultHistory {
    boolean matches(String patterns, String line) {
      return matchPatterns(patterns, line);
    }
  }
}

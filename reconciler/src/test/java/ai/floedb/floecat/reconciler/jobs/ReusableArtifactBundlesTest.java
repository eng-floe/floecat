/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package ai.floedb.floecat.reconciler.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundlePayload;
import org.junit.jupiter.api.Test;

class ReusableArtifactBundlesTest {
  @Test
  void acceptsCurrentFormatAndRejectsOtherVersions() throws Exception {
    byte[] current =
        ReusableArtifactBundlePayload.newBuilder().setFormatVersion(1).build().toByteArray();
    byte[] unsupported =
        ReusableArtifactBundlePayload.newBuilder().setFormatVersion(2).build().toByteArray();

    assertEquals(1, ReusableArtifactBundles.parse(current).getFormatVersion());
    assertThrows(IllegalArgumentException.class, () -> ReusableArtifactBundles.parse(unsupported));
  }
}

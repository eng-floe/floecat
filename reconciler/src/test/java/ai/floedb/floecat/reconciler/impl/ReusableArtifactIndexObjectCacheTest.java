/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexObjectReference;
import com.google.protobuf.ByteString;
import java.security.MessageDigest;
import org.junit.jupiter.api.Test;

class ReusableArtifactIndexObjectCacheTest {
  @Test
  void getReturnsAnIsolatedCopy() throws Exception {
    byte[] original = {1, 2, 3};
    var reference =
        ReusableArtifactIndexObjectReference.newBuilder()
            .setUri("/index-object.pb")
            .setPayloadBytes(original.length)
            .setPayloadSha256(
                ByteString.copyFrom(MessageDigest.getInstance("SHA-256").digest(original)))
            .build();
    var cache = new ReusableArtifactIndexObjectCache();
    cache.put(reference.getUri(), original);

    byte[] first = cache.get(reference);
    first[0] = 99;

    assertArrayEquals(original, cache.get(reference));
  }
}

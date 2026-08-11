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

import ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundlePayload;
import com.google.protobuf.InvalidProtocolBufferException;

/** Shared decoding rules for compact reusable artifact bundles. */
public final class ReusableArtifactBundles {
  public static final int FORMAT_VERSION = 1;

  private ReusableArtifactBundles() {}

  public static ReusableArtifactBundlePayload parse(byte[] bytes)
      throws InvalidProtocolBufferException {
    ReusableArtifactBundlePayload payload = ReusableArtifactBundlePayload.parseFrom(bytes);
    if (payload.getFormatVersion() != FORMAT_VERSION) {
      throw new IllegalArgumentException(
          "unsupported reusable artifact bundle format version: " + payload.getFormatVersion());
    }
    return payload;
  }
}

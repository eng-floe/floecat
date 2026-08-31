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
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

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

  public static List<ReusableArtifactBundleSelection> inheritedIndexArtifactBundleSelections(
      Iterable<ReconcileFileGroupTask> plannedGroups) {
    Map<String, StatsObjectDescriptor> inherited = new LinkedHashMap<>();
    Map<String, LinkedHashSet<String>> selectedPaths = new LinkedHashMap<>();
    for (ReconcileFileGroupTask group : plannedGroups) {
      for (ReconcileFileExecutionPlan plan : group.fileExecutionPlans()) {
        for (ReusableArtifactBundleSelection selection : plan.reusableArtifactBundleSelections()) {
          if (selection.indexFilePaths().isEmpty()
              || !selection.payloadUri().startsWith("/accounts/")) {
            continue;
          }
          if (selection.targetStorageId().isBlank()
              || selection.payloadBytes() <= 0L
              || selection.payloadSha256().length != 32
              || !ReusableArtifactBundleUris.isBundleUri(selection.payloadUri())
              || !ReusableArtifactBundleUris.matchesDigest(
                  selection.payloadUri(), selection.payloadSha256())) {
            throw new IllegalArgumentException("invalid inherited index artifact bundle selection");
          }
          StatsObjectDescriptor descriptor =
              StatsObjectDescriptor.newBuilder()
                  .setTargetStorageId(selection.targetStorageId())
                  .setPayloadUri(selection.payloadUri())
                  .setPayloadBytes(selection.payloadBytes())
                  .setPayloadSha256(ByteString.copyFrom(selection.payloadSha256()))
                  .build();
          StatsObjectDescriptor prior = inherited.putIfAbsent(selection.payloadUri(), descriptor);
          if (prior != null && !prior.equals(descriptor)) {
            throw new IllegalArgumentException(
                "conflicting inherited index artifact bundle selection");
          }
          selectedPaths
              .computeIfAbsent(selection.payloadUri(), ignored -> new LinkedHashSet<>())
              .addAll(selection.indexFilePaths());
        }
      }
    }
    return inherited.values().stream()
        .map(
            descriptor ->
                new ReusableArtifactBundleSelection(
                    descriptor.getTargetStorageId(),
                    descriptor.getPayloadUri(),
                    descriptor.getPayloadBytes(),
                    descriptor.getPayloadSha256().toByteArray(),
                    List.of(),
                    List.copyOf(selectedPaths.get(descriptor.getPayloadUri()))))
        .toList();
  }

  public static List<StatsObjectDescriptor> inheritedIndexArtifactBundles(
      Iterable<ReconcileFileGroupTask> plannedGroups) {
    return inheritedIndexArtifactBundleSelections(plannedGroups).stream()
        .map(
            selection ->
                StatsObjectDescriptor.newBuilder()
                    .setTargetStorageId(selection.targetStorageId())
                    .setPayloadUri(selection.payloadUri())
                    .setPayloadBytes(selection.payloadBytes())
                    .setPayloadSha256(ByteString.copyFrom(selection.payloadSha256()))
                    .build())
        .toList();
  }
}

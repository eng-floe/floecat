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

package ai.floedb.floecat.service.repo.model;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.PointerReferenceKind;
import ai.floedb.floecat.common.rpc.ResourceId;

public final class PointerReferences {
  private PointerReferences() {}

  public static Pointer.Builder asBlobPointer(Pointer.Builder builder, String blobUri) {
    return builder
        .setBlobUri(blankToEmpty(blobUri))
        .setReferenceKind(PointerReferenceKind.PRK_BLOB_URI);
  }

  public static Pointer.Builder asInlineJsonPointer(Pointer.Builder builder, String payload) {
    return builder
        .setBlobUri(blankToEmpty(payload))
        .setReferenceKind(PointerReferenceKind.PRK_INLINE_JSON);
  }

  public static Pointer.Builder asPointerKeyPointer(Pointer.Builder builder, String pointerKey) {
    return builder
        .setBlobUri(blankToEmpty(pointerKey))
        .setReferenceKind(PointerReferenceKind.PRK_POINTER_KEY);
  }

  public static Pointer.Builder asOpaqueMarkerPointer(Pointer.Builder builder, String payload) {
    return builder
        .setBlobUri(blankToEmpty(payload))
        .setReferenceKind(PointerReferenceKind.PRK_OPAQUE_MARKER);
  }

  public static Pointer blobPointer(String key, String blobUri, long version) {
    return asBlobPointer(
            Pointer.newBuilder().setKey(blankToEmpty(key)).setVersion(version), blobUri)
        .build();
  }

  public static Pointer blobPointer(
      String key, String blobUri, long version, ResourceId resourceId, String displayName) {
    return asBlobPointer(
            Pointer.newBuilder()
                .setKey(blankToEmpty(key))
                .setVersion(version)
                .setResourceId(resourceId)
                .setDisplayName(blankToEmpty(displayName)),
            blobUri)
        .build();
  }

  public static Pointer inlineJsonPointer(String key, String payload, long version) {
    return asInlineJsonPointer(
            Pointer.newBuilder().setKey(blankToEmpty(key)).setVersion(version), payload)
        .build();
  }

  public static Pointer pointerKeyPointer(String key, String pointerKey, long version) {
    return asPointerKeyPointer(
            Pointer.newBuilder().setKey(blankToEmpty(key)).setVersion(version), pointerKey)
        .build();
  }

  public static Pointer opaqueMarkerPointer(String key, String payload, long version) {
    return asOpaqueMarkerPointer(
            Pointer.newBuilder().setKey(blankToEmpty(key)).setVersion(version), payload)
        .build();
  }

  public static boolean isBlobPointer(Pointer pointer) {
    if (pointer == null) {
      return false;
    }
    PointerReferenceKind kind = pointer.getReferenceKind();
    // Legacy pointers predate reference_kind and should continue to resolve as blob pointers.
    return kind == PointerReferenceKind.PRK_BLOB_URI
        || kind == PointerReferenceKind.PRK_UNSPECIFIED;
  }

  public static boolean isInlineJsonPointer(Pointer pointer) {
    return hasKind(pointer, PointerReferenceKind.PRK_INLINE_JSON);
  }

  public static boolean isPointerKeyPointer(Pointer pointer) {
    return hasKind(pointer, PointerReferenceKind.PRK_POINTER_KEY);
  }

  public static boolean isOpaqueMarkerPointer(Pointer pointer) {
    return hasKind(pointer, PointerReferenceKind.PRK_OPAQUE_MARKER);
  }

  public static boolean hasKind(Pointer pointer, PointerReferenceKind expectedKind) {
    return pointer != null && expectedKind != null && pointer.getReferenceKind() == expectedKind;
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }
}

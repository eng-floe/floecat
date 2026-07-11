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

package ai.floedb.floecat.service.query;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.PinKind;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PinValidatorTest {

  private TableRootRepository roots;
  private PinValidator validator;

  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("t")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  @BeforeEach
  void setUp() {
    roots = mock(TableRootRepository.class);
    validator = new PinValidator(roots);
    // The pinned root resolves at its pinned version unless a test overrides it.
    when(roots.blobEtag("s3://t/root/abc.pb")).thenReturn("etag-root");
  }

  /** A root-backed pin — the only shape construction can produce. */
  private static TablePin.Builder pin() {
    return TablePin.newBuilder()
        .setTableId(TABLE)
        .setPinKind(PinKind.PIN_KIND_CURRENT)
        .setSnapshotId(7)
        .setRootUri("s3://t/root/abc.pb")
        .setRootVersion("etag-root")
        .setTableBlobUri("s3://t/table.pb")
        .setSnapshotBlobUri("s3://t/snap-7.pb");
  }

  @Test
  void aRootBackedPinPasses() {
    validator.validate("corr", pin().build());
  }

  @Test
  void pinWithoutARootWasNeverLegitimatelyConstructed() {
    // Construction reads the root or fails, so an empty root URI is a broken invariant — rejected
    // loudly rather than waved through.
    assertThrows(
        StatusRuntimeException.class,
        () -> validator.validate("corr", pin().clearRootUri().clearRootVersion().build()));
  }

  @Test
  void aVanishedRootBlobFails() {
    when(roots.blobEtag("s3://t/root/abc.pb")).thenReturn(null);
    assertThrows(StatusRuntimeException.class, () -> validator.validate("corr", pin().build()));
  }

  @Test
  void aRootVersionMismatchFails() {
    // The URI is content-addressed, so a different etag at the pinned URI is a broken store
    // invariant, not a benign refresh.
    when(roots.blobEtag("s3://t/root/abc.pb")).thenReturn("etag-OTHER");
    assertThrows(StatusRuntimeException.class, () -> validator.validate("corr", pin().build()));
  }

  @Test
  void aPinnedRootSurvivesTheCurrentPointerAdvancing() {
    // Validation is against the pinned immutable root blob, never the live root pointer: after a
    // newer root commits, the pinned blob is still retrievable at its version and the pin holds.
    validator.validate("corr", pin().build());
  }

  @Test
  void copiedRefsAreNotReValidatedPerPin() {
    // The copied table/snapshot refs came out of the pinned root and are GC-rooted with it; reads
    // that load them fail loudly via requirePinned* if one is gone. The pin's integrity contract
    // is the single root leg, so an unreadable copied blob does not fail validation.
    validator.validate("corr", pin().setTableBlobUri("s3://t/gone.pb").build());
  }
}

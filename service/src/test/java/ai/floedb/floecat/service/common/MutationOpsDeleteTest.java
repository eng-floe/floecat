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

package ai.floedb.floecat.service.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MutationOpsDeleteTest {
  private static final MutationMeta INITIAL =
      MutationMeta.newBuilder().setPointerVersion(7L).build();

  @Test
  void unconditionalDeleteRetriesWhenConcurrentMutationRemains() {
    MutationMeta current = MutationMeta.newBuilder().setPointerVersion(8L).build();

    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () ->
            MutationOps.deleteWithPreconditions(
                () -> INITIAL,
                Precondition.getDefaultInstance(),
                ignored -> false,
                () -> current,
                "corr",
                "table",
                Map.of("id", "table-1")));
  }

  @Test
  void unconditionalDeleteRetriesRepositoryPreconditionFailure() {
    MutationMeta current = MutationMeta.newBuilder().setPointerVersion(8L).build();

    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () ->
            MutationOps.deleteWithPreconditions(
                () -> INITIAL,
                Precondition.getDefaultInstance(),
                ignored -> {
                  throw new BaseResourceRepository.PreconditionFailedException("lost race");
                },
                () -> current,
                "corr",
                "table",
                Map.of("id", "table-1")));
  }

  @Test
  void unconditionalDeleteSucceedsWhenConcurrentDeleteWon() {
    MutationMeta absent = MutationMeta.getDefaultInstance();

    MutationMeta result =
        MutationOps.deleteWithPreconditions(
            () -> INITIAL,
            Precondition.getDefaultInstance(),
            ignored -> false,
            () -> absent,
            "corr",
            "table",
            Map.of("id", "table-1"));

    assertSame(absent, result);
  }

  @Test
  void conditionalDeleteStillReportsVersionMismatch() {
    MutationMeta current = MutationMeta.newBuilder().setPointerVersion(8L).build();
    Precondition precondition = Precondition.newBuilder().setExpectedVersion(7L).build();

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                MutationOps.deleteWithPreconditions(
                    () -> INITIAL,
                    precondition,
                    ignored -> false,
                    () -> current,
                    "corr",
                    "table",
                    Map.of("id", "table-1")));

    assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
  }
}

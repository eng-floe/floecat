/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.common.rpc.PageRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.junit.jupiter.api.Test;

class CatalogDiscoveryPagesTest {
  @Test
  void pagesAndBindsContinuationToTheRequestContext() {
    var first =
        CatalogDiscoveryPages.page(
            List.of("a", "b", "c"),
            PageRequest.newBuilder().setPageSize(2).build(),
            "integration-generation-parent",
            "corr");

    assertEquals(List.of("a", "b"), first.values());
    assertFalse(first.nextToken().isBlank());
    assertEquals(3, first.totalSize());

    var second =
        CatalogDiscoveryPages.page(
            List.of("a", "b", "c"),
            PageRequest.newBuilder().setPageSize(2).setPageToken(first.nextToken()).build(),
            "integration-generation-parent",
            "corr");
    assertEquals(List.of("c"), second.values());

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                CatalogDiscoveryPages.page(
                    List.of("a", "b", "c"),
                    PageRequest.newBuilder().setPageToken(first.nextToken()).build(),
                    "different-parent",
                    "corr"));
    assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
  }
}

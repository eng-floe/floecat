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

package ai.floedb.floecat.service.error.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.storage.errors.SourceCatalogVendingGrpcStatus;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import org.junit.jupiter.api.Test;

class MessageCatalogTest {
  @Test
  void aVendingDiagnosticSurvivesRendering() {
    // The gap that let two interceptor surprises through: every assertion on these statuses was
    // made on the pre-interceptor exception, and LocalizeErrorsInterceptor rewrites the message
    // with MessageCatalog.render, which never reads Error.message. Rendered here through the real
    // catalog, so a keyed template going missing fails a test rather than reaching an operator as
    // "Precondition failed."
    String diagnostic =
        "source-catalog vending refused: Catalog Integration integration-1 does not support"
            + " storage credential vending";
    var refusal = SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused(diagnostic);
    var detail = errorDetailOf(refusal);

    assertEquals(diagnostic, new MessageCatalog(Locale.ENGLISH).render(detail));
  }

  @Test
  void everyVendingReasonRendersItsDiagnosticRatherThanTheBareCode() {
    record Case(String name, io.grpc.StatusRuntimeException error) {}
    for (var c :
        java.util.List.of(
            new Case(
                "refused",
                SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused("refused-detail")),
            new Case(
                "unauthenticated",
                SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused(
                    io.grpc.Status.Code.UNAUTHENTICATED,
                    ErrorCode.MC_UNAUTHENTICATED,
                    "unauthenticated-detail",
                    null)),
            new Case(
                "forbidden",
                SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused(
                    io.grpc.Status.Code.PERMISSION_DENIED,
                    ErrorCode.MC_PERMISSION_DENIED,
                    "forbidden-detail",
                    null)),
            new Case(
                "unavailable",
                SourceCatalogVendingGrpcStatus.sourceCatalogVendUnavailable(
                    "unavailable-detail", null)),
            new Case(
                "not-refreshable",
                SourceCatalogVendingGrpcStatus.vendedCredentialsNotRefreshable(
                    "not-refreshable-detail")),
            new Case(
                "no-authority",
                SourceCatalogVendingGrpcStatus.noMatchingStorageAuthority(
                    "no-authority-detail")))) {
      String rendered = new MessageCatalog(Locale.ENGLISH).render(errorDetailOf(c.error()));

      assertTrue(rendered.endsWith("-detail"), c.name() + " rendered as: " + rendered);
    }
  }

  private static ai.floedb.floecat.common.rpc.Error errorDetailOf(
      io.grpc.StatusRuntimeException error) {
    var status = io.grpc.protobuf.StatusProto.fromThrowable(error);
    for (var any : status.getDetailsList()) {
      if (any.is(ai.floedb.floecat.common.rpc.Error.class)) {
        try {
          return any.unpack(ai.floedb.floecat.common.rpc.Error.class);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          throw new AssertionError(e);
        }
      }
    }
    throw new AssertionError("no floecat Error detail on " + error.getStatus());
  }

  @Test
  void everyMessageKeyExistsInBundle() {
    final ResourceBundle bundle;
    try {
      bundle = ResourceBundle.getBundle("errors", Locale.ENGLISH);
    } catch (MissingResourceException e) {
      fail("errors_en.properties missing or unreadable", e);
      return;
    }
    for (GeneratedErrorMessages.MessageKey key : GeneratedErrorMessages.MessageKey.values()) {
      assertTrue(
          bundle.containsKey(key.fullKey()),
          () -> key.fullKey() + " missing in errors_en.properties");
    }
  }

  @Test
  void everyErrorCodeBaseKeyExistsInBundle() {
    final ResourceBundle bundle;
    try {
      bundle = ResourceBundle.getBundle("errors", Locale.ENGLISH);
    } catch (MissingResourceException e) {
      fail("errors_en.properties missing or unreadable", e);
      return;
    }
    for (ErrorCode code : ErrorCode.values()) {
      if (code == ErrorCode.UNRECOGNIZED) {
        continue;
      }
      assertTrue(
          bundle.containsKey(code.name()), () -> code.name() + " missing in errors_en.properties");
    }
  }
}

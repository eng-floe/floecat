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

package ai.floedb.floecat.service.connector.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.spi.DatabricksAccessDelegation;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The create/update/validate rejection for an unrecognized vend opt-in. All three request paths
 * call the same validator, so it is exercised directly rather than through one of them.
 */
class ConnectorsImplPropertyValidationTest {

  private static final String OPTION = DatabricksAccessDelegation.VEND_OPTION;

  private static StatusRuntimeException reject(ConnectorKind kind, Map<String, String> properties) {
    return assertThrows(
        StatusRuntimeException.class,
        () -> ConnectorsImpl.validateConnectorProperties(kind, properties, "corr-1"));
  }

  @Test
  void anUnrecognizedVendOptionIsRejectedWithoutEchoingIt() {
    // Shaped like the thing this property is a plausible place to paste by mistake, which is why
    // the value is never echoed. A near-miss spelling would not do: "vended-credential" is a
    // prefix of an accepted value, so asserting its absence would fail on the accepted list.
    var failure = reject(ConnectorKind.CK_DELTA, Map.of(OPTION, "AKIAIOSFODNN7EXAMPLE"));

    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(failure.getStatus().getDescription())
        .contains(OPTION)
        .contains(DatabricksAccessDelegation.acceptedValuesDescription())
        .doesNotContain("AKIAIOSFODNN7EXAMPLE");
  }

  @Test
  void aKeyThatOnlyLooksLikeTheVendOptionIsRefused() {
    // The lookup is exact, so a near-miss key would read as absent, both gates would agree the
    // connector never opted in, and the mistake would surface as reads falling back to an authority
    // nobody configured -- the failure this validator exists to prevent.
    for (String key :
        new String[] {
          "databricks.access_delegation",
          "Databricks.Access-Delegation",
          " databricks.access-delegation",
          // The env-var shape. Folding only underscores leaves this comparing against the dotted
          // spelling, so it matched nothing and both opt-in gates read "never opted in".
          "databricks_access_delegation",
          "databricks.access.delegation",
          "DATABRICKS_ACCESS_DELEGATION"
        }) {
      var failure =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  ConnectorsImpl.validateConnectorProperties(
                      ConnectorKind.CK_DELTA, Map.of(key, "vended-credentials"), "corr-1"),
              key);

      assertThat(failure.getStatus().getCode()).as(key).isEqualTo(Status.Code.INVALID_ARGUMENT);
      // The sentence an operator reads has to name their typo and the spelling that works; the
      // generic invalid-argument text carries the key in structured params only.
      assertThat(failure.getStatus().getDescription())
          .as(key)
          .contains(key.trim())
          .contains(OPTION);
    }

    // The canonical spelling is of course fine, and so is an unrelated property.
    assertThatCode(
            () ->
                ConnectorsImpl.validateConnectorProperties(
                    ConnectorKind.CK_DELTA,
                    Map.of(OPTION, "vended-credentials", "delta.source", "unity"),
                    "corr-1"))
        .doesNotThrowAnyException();
  }

  @Test
  void aMisspelledKeyIsNamedWithoutItsSurroundingWhitespace() {
    // The canonicalization trims, so the match pins the key's interior to the option but not what
    // wraps it. The key is carried into the Error detail's params unclamped -- the status
    // description is safe by accident, because GrpcErrors.clampDetail collapses \s+ -- so CR/LF
    // reaches whatever reads params and forges a line there.
    //
    // Only whitespace can get this far: entry to the branch requires key.trim() to equal the
    // option, and trim strips exactly the characters <= U+0020, so a key wrapped in U+2028 or
    // U+0085 never matches in the first place. Trim on the way out is therefore complete.
    String wrapped = "\r\n" + "databricks.access_delegation" + "\t";

    var failure = reject(ConnectorKind.CK_DELTA, Map.of(wrapped, "vended-credentials"));

    assertThat(errorParams(failure))
        .containsEntry("key", "databricks.access_delegation")
        .containsEntry("canonical", OPTION);
  }

  private static Map<String, String> errorParams(StatusRuntimeException ex) {
    com.google.rpc.Status status = StatusProto.fromThrowable(ex);
    if (status == null) {
      throw new AssertionError("missing rpc status details");
    }
    for (com.google.protobuf.Any detail : status.getDetailsList()) {
      if (detail.is(ai.floedb.floecat.common.rpc.Error.class)) {
        try {
          return detail.unpack(ai.floedb.floecat.common.rpc.Error.class).getParamsMap();
        } catch (Exception e) {
          throw new AssertionError("failed to unpack error details", e);
        }
      }
    }
    throw new AssertionError("missing error detail payload");
  }

  @Test
  void everyRecognizedVendOptionIsAccepted() {
    // Blank and absent are how a cleared property and an unset one arrive; both mean "not opted
    // in" and must not fail creation.
    for (String value : DatabricksAccessDelegation.acceptedValues()) {
      assertThatCode(
              () ->
                  ConnectorsImpl.validateConnectorProperties(
                      ConnectorKind.CK_DELTA, Map.of(OPTION, value), "corr-1"))
          .as(value)
          .doesNotThrowAnyException();
    }
    for (Map<String, String> properties :
        java.util.List.of(Map.<String, String>of(), Map.of(OPTION, ""), Map.of(OPTION, "   "))) {
      assertThatCode(
              () ->
                  ConnectorsImpl.validateConnectorProperties(
                      ConnectorKind.CK_DELTA, properties, "corr-1"))
          .as("%s", properties)
          .doesNotThrowAnyException();
    }
    assertThatCode(
            () ->
                ConnectorsImpl.validateConnectorProperties(ConnectorKind.CK_DELTA, null, "corr-1"))
        .doesNotThrowAnyException();
  }

  @Test
  void theVendOptionIsOnlyCheckedForDelta() {
    // Nothing else reads the option, so rejecting it elsewhere would fail connectors the value has
    // no effect on.
    for (ConnectorKind kind : ConnectorKind.values()) {
      if (kind == ConnectorKind.CK_DELTA || kind == ConnectorKind.UNRECOGNIZED) {
        continue;
      }
      assertThatCode(
              () ->
                  ConnectorsImpl.validateConnectorProperties(
                      kind, Map.of(OPTION, "vended-credential"), "corr-1"))
          .as("%s", kind)
          .doesNotThrowAnyException();
    }
  }

  @Test
  void aBadVendValueIsReportedAheadOfAForbiddenSecretKey() {
    // Which of the two an operator sees when both are wrong. The vend error is the actionable one:
    // it names accepted values, where the secret-key error only says the property is not allowed.
    //
    // This does not pin the ordering the validator's own comment warns about -- dropping CK_DELTA
    // from CONNECTOR_KINDS_WITH_FORBIDDEN_SECRET_PROPERTIES is not observable from any input while
    // the vend check runs first, so that one is held by reading the code, not by this test.
    Map<String, String> properties = new HashMap<>();
    properties.put(OPTION, "vended-credential");
    properties.put("s3.secret-access-key", "irrelevant");

    var failure = reject(ConnectorKind.CK_DELTA, properties);

    assertThat(failure.getStatus().getDescription())
        .contains(OPTION)
        .doesNotContain("s3.secret-access-key");
  }
}

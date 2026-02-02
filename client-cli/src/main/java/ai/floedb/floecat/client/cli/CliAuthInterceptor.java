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
package ai.floedb.floecat.client.cli;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.function.Supplier;

final class CliAuthInterceptor implements ClientInterceptor {

  private static final Metadata.Key<byte[]> PRINC_BIN =
      Metadata.Key.of("x-principal-bin", Metadata.BINARY_BYTE_MARSHALLER);

  private final Supplier<String> tokenSupplier;
  private final Supplier<String> accountSupplier;
  private final Supplier<String> sessionHeaderNameSupplier;

  CliAuthInterceptor(
      Supplier<String> tokenSupplier,
      Supplier<String> accountSupplier,
      Supplier<String> sessionHeaderNameSupplier) {
    this.tokenSupplier = tokenSupplier;
    this.accountSupplier = accountSupplier;
    this.sessionHeaderNameSupplier = sessionHeaderNameSupplier;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

    return new ForwardingClientCall.SimpleForwardingClientCall<>(
        next.newCall(method, callOptions)) {

      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        String token = nvl(tokenSupplier.get()).trim();

        if (!token.isBlank()) {
          // Default to Bearer if the user provided a raw token
          String value =
              token.regionMatches(true, 0, "bearer ", 0, 7) ? token : ("Bearer " + token);

          String headerName = nvl(sessionHeaderNameSupplier.get()).trim();
          if (headerName.isBlank()) {
            headerName = "Authorization";
          }
          headers.put(Metadata.Key.of(headerName, Metadata.ASCII_STRING_MARSHALLER), value);

        } else {
          // Dev mode: send principal header derived from currentAccountId
          String acct = nvl(accountSupplier.get()).trim();
          if (!acct.isBlank()) {
            // Dev mode: allow basic exploration without an external token.
            // The server authorizer checks PrincipalContext.permissions.
            PrincipalContext pc =
                PrincipalContext.newBuilder()
                    .setAccountId(acct)
                    .setSubject("cli")
                    .setLocale("en")
                    // Include a role for observability/debugging; server-side role mapping may
                    // differ.
                    .addRoles("developer")
                    // Minimum read permissions to make catalogs/namespaces/tables usable.
                    .addPermissions("account.read")
                    .addPermissions("catalog.read")
                    .addPermissions("namespace.read")
                    .addPermissions("table.read")
                    .addPermissions("view.read")
                    // Include common write/manage perms so dev environments don't get stuck.
                    .addPermissions("account.write")
                    .addPermissions("catalog.write")
                    .addPermissions("namespace.write")
                    .addPermissions("table.write")
                    .addPermissions("view.write")
                    .addPermissions("connector.manage")
                    .build();
            headers.put(PRINC_BIN, pc.toByteArray());
          }
        }

        super.start(responseListener, headers);
      }
    };
  }

  private static String nvl(String s) {
    return s == null ? "" : s;
  }
}

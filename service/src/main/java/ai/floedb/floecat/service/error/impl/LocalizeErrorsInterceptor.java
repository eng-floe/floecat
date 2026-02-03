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

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import com.google.protobuf.Any;
import com.google.rpc.Status;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.protobuf.StatusProto;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Locale;
import java.util.Optional;

@GlobalInterceptor
@ApplicationScoped
@Priority(10)
public class LocalizeErrorsInterceptor implements ServerInterceptor {
  private static final Metadata.Key<String> ACCEPT_LANGUAGE =
      Metadata.Key.of("accept-language", Metadata.ASCII_STRING_MARSHALLER);

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    PrincipalContext principal = InboundContextInterceptor.PC_KEY.get();

    String tag = pickLocaleTag(headers, principal);
    final Locale locale =
        Optional.ofNullable(Locale.forLanguageTag(tag))
            .filter(l -> !l.toLanguageTag().isEmpty())
            .orElse(Locale.ENGLISH);

    return next.startCall(
        new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
          @Override
          public void close(io.grpc.Status status, Metadata trailers) {
            try {
              var exception = status.asRuntimeException(trailers);
              var statusProto = StatusProto.fromThrowable(exception);
              if (status != null) {
                Error error = extract(statusProto);
                if (error != null) {
                  String rendered = new MessageCatalog(locale).render(error);
                  var newError = Error.newBuilder(error).setMessage(rendered).build();
                  var newStatus =
                      com.google.rpc.Status.newBuilder(statusProto)
                          .setMessage(rendered)
                          .clearDetails()
                          .addDetails(Any.pack(newError))
                          .build();

                  var localeTrailers =
                      StatusProto.toStatusRuntimeException(newStatus).getTrailers();

                  Metadata metadata = new Metadata();
                  metadata.merge(trailers);
                  if (localeTrailers != null) {
                    metadata.merge(localeTrailers);
                  }

                  super.close(status.withDescription(rendered), metadata);
                  return;
                }
              }
            } catch (Throwable ignore) {
              // ignore
            }
            super.close(status, trailers);
          }
        },
        headers);
  }

  private static Error extract(Status status) throws Exception {
    for (Any any : status.getDetailsList()) {
      if (any.is(Error.class)) {
        return any.unpack(Error.class);
      }
    }
    return null;
  }

  private static String pickLocaleTag(Metadata headers, PrincipalContext principal) {
    String acceptLanguage = headers.get(ACCEPT_LANGUAGE);
    if (acceptLanguage != null && !acceptLanguage.isBlank()) {
      String first = acceptLanguage.split(",", 2)[0].trim();
      int qequalsToken = first.indexOf(";q=");

      if (qequalsToken > 0) {
        first = first.substring(0, qequalsToken).trim();
      }

      if (!first.isBlank()) {
        return first;
      }
    }

    if (principal != null && !principal.getLocale().isBlank()) {
      return principal.getLocale();
    }

    return "en";
  }
}

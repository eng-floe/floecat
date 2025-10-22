package ai.floedb.metacat.service.error.impl;

import ai.floedb.metacat.common.rpc.Error;
import ai.floedb.metacat.common.rpc.PrincipalContext;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;
import io.grpc.*;
import io.grpc.protobuf.StatusProto;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Locale;
import java.util.Optional;

@GlobalInterceptor
@ApplicationScoped
public class LocalizeErrorsInterceptor implements ServerInterceptor {
  private static final Metadata.Key<byte[]> PRINC_BIN =
      Metadata.Key.of("x-principal-bin", Metadata.BINARY_BYTE_MARSHALLER);
  private static final Metadata.Key<String> ACCEPT_LANGUAGE =
      Metadata.Key.of("accept-language", Metadata.ASCII_STRING_MARSHALLER);

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    byte[] principalContextBytes = headers.get(PRINC_BIN);
    PrincipalContext principal = tryParsePrincipal(principalContextBytes);

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

  private static PrincipalContext tryParsePrincipal(byte[] pcBytes) {
    if (pcBytes == null || pcBytes.length == 0) {
      return null;
    }

    try {
      return PrincipalContext.parseFrom(pcBytes);
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }
}

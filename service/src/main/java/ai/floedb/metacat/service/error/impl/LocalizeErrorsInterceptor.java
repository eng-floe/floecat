package ai.floedb.metacat.service.error.impl;

import java.util.Locale;
import java.util.Optional;

import javax.management.relation.InvalidRoleInfoException;

import jakarta.enterprise.context.ApplicationScoped;
import io.grpc.*;
import io.quarkus.grpc.GlobalInterceptor;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;
import io.grpc.protobuf.StatusProto;
import ai.floedb.metacat.common.rpc.Error;
import ai.floedb.metacat.common.rpc.PrincipalContext;

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

    byte[] pcBytes = headers.get(PRINC_BIN);
    PrincipalContext principal = tryParsePrincipal(pcBytes);
    String tag = pickLocaleTag(headers, principal);
    final Locale locale = Optional.ofNullable(Locale.forLanguageTag(tag))
      .filter(l -> !l.toLanguageTag().isEmpty())
      .orElse(Locale.ENGLISH);

    return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
      @Override
      public void close(io.grpc.Status status, Metadata trailers) {
        try {
          var sre = status.asRuntimeException(trailers);
          var st = StatusProto.fromThrowable(sre);
          if (st != null) {
            Error app = extract(st);
            if (app != null) {
              String rendered = new MessageCatalog(locale).render(app);
              var newErr = Error.newBuilder(app).setMessage(rendered).build();
              var newSt  = com.google.rpc.Status.newBuilder(st)
                .setMessage(rendered)
                .clearDetails().addDetails(Any.pack(newErr))
                .build();

              var locTrailers = StatusProto.toStatusRuntimeException(newSt).getTrailers();

              Metadata out = new Metadata();
              out.merge(trailers);
              if (locTrailers != null) out.merge(locTrailers);

              super.close(status.withDescription(rendered), out);
              return;
            }
          }
        } catch (Throwable ignore) {}
        super.close(status, trailers);
      }
    }, headers);
  }

  private static Error extract(Status st) throws Exception {
    for (Any any : st.getDetailsList()) {
      if (any.is(Error.class)) return any.unpack(Error.class);
    }
    return null;
  }

  private static String pickLocaleTag(Metadata headers, PrincipalContext principal) {
    String accept = headers.get(ACCEPT_LANGUAGE);
    if (accept != null && !accept.isBlank()) {
      String first = accept.split(",", 2)[0].trim();
      int q = first.indexOf(";q=");
      if (q > 0) first = first.substring(0, q).trim();
      if (!first.isBlank()) return first;
    }
    if (principal != null && !principal.getLocale().isBlank()) {
      return principal.getLocale();
    }
    return "en";
  }

  private static PrincipalContext tryParsePrincipal(byte[] pcBytes) {
    if (pcBytes == null || pcBytes.length == 0) return null;
    try {
      return PrincipalContext.parseFrom(pcBytes);
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }
}
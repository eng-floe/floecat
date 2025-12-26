package ai.floedb.floecat.gateway.iceberg.rest.common;

import jakarta.ws.rs.ext.Provider;
import jakarta.ws.rs.ext.ReaderInterceptor;
import jakarta.ws.rs.ext.ReaderInterceptorContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@Provider
public class RequestBodyLoggingFilter implements ReaderInterceptor {
  private static final Logger LOG = Logger.getLogger(RequestBodyLoggingFilter.class);
  private static final int MAX_BYTES = 64 * 1024;

  @ConfigProperty(name = "floecat.rest.log-request-body", defaultValue = "false")
  boolean logRequestBody;

  @Override
  public Object aroundReadFrom(ReaderInterceptorContext context) throws IOException {
    if (!logRequestBody) {
      return context.proceed();
    }
    String contentType = context.getHeaders().getFirst("Content-Type");
    if (contentType == null || !contentType.startsWith("application/json")) {
      return context.proceed();
    }
    InputStream in = context.getInputStream();
    if (in == null) {
      return context.proceed();
    }
    byte[] body = readUpTo(in, MAX_BYTES);
    context.setInputStream(new ByteArrayInputStream(body));
    String payload = new String(body, StandardCharsets.UTF_8);
    if (body.length >= MAX_BYTES) {
      payload = payload + "...(truncated)";
    }
    LOG.infof("Request body payload=%s", payload);
    return context.proceed();
  }

  private static byte[] readUpTo(InputStream in, int limit) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    int total = 0;
    int read;
    while ((read = in.read(buffer)) != -1) {
      int remaining = limit - total;
      if (remaining <= 0) {
        break;
      }
      int toWrite = Math.min(read, remaining);
      out.write(buffer, 0, toWrite);
      total += toWrite;
    }
    return out.toByteArray();
  }
}

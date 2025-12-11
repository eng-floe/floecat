package ai.floedb.floecat.gateway.iceberg.rest.common;

import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.util.stream.Collectors;

@Provider
public class ConstraintViolationExceptionMapper
    implements ExceptionMapper<ConstraintViolationException> {

  @Override
  public Response toResponse(ConstraintViolationException exception) {
    String message =
        exception.getConstraintViolations().stream()
            .map(this::formatViolation)
            .filter(s -> s != null && !s.isBlank())
            .collect(Collectors.joining("; "));
    if (message.isBlank()) {
      message = "Validation failed";
    }
    return IcebergErrorResponses.validation(message);
  }

  private String formatViolation(ConstraintViolation<?> violation) {
    if (violation == null) {
      return "";
    }
    String path = violation.getPropertyPath() == null ? "" : violation.getPropertyPath().toString();
    if (path == null || path.isBlank()) {
      return violation.getMessage();
    }
    return path + " " + violation.getMessage();
  }
}

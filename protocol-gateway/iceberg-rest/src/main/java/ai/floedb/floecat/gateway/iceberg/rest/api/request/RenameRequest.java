package ai.floedb.floecat.gateway.iceberg.rest.api.request;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;

public record RenameRequest(
    @NotNull @Valid TableIdentifierBody source, @NotNull @Valid TableIdentifierBody destination) {
  public record TableIdentifierBody(@NotNull List<String> namespace, @NotBlank String name) {}
}

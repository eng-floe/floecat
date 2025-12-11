package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;

public record TableIdentifierDto(@NotNull List<String> namespace, @NotBlank String name) {}

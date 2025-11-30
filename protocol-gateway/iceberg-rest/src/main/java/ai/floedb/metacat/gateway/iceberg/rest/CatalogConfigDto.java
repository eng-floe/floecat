package ai.floedb.metacat.gateway.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CatalogConfigDto(
    Map<String, String> defaults,
    Map<String, String> overrides,
    List<String> endpoints,
    @JsonProperty("idempotency-key-lifetime") String idempotencyKeyLifetime) {}

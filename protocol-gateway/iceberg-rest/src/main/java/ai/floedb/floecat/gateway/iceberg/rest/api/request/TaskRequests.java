package ai.floedb.floecat.gateway.iceberg.rest.api.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class TaskRequests {
  private TaskRequests() {}

  public record Fetch(@JsonProperty("plan-task") String planTask) {}
}

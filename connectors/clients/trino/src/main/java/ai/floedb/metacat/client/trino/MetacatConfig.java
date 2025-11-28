package ai.floedb.metacat.client.trino;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

public class MetacatConfig {

  private String metacatUri;
  private boolean coordinatorFileCaching = false;

  @NotNull
  public String getMetacatUri() {
    return metacatUri;
  }

  @Config("metacat.uri")
  public MetacatConfig setMetacatUri(String uri) {
    this.metacatUri = uri;
    return this;
  }

  @Config("metacat.coordinator-file-caching")
  public MetacatConfig setCoordinatorFileCaching(boolean enabled) {
    this.coordinatorFileCaching = enabled;
    return this;
  }

  public boolean isCoordinatorFileCaching() {
    return coordinatorFileCaching;
  }
}

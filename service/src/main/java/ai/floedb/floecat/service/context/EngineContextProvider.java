package ai.floedb.floecat.service.context;

import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public final class EngineContextProvider {

  public String engineKind() {
    return InboundContextInterceptor.ENGINE_KIND_KEY.get();
  }

  public String engineVersion() {
    return InboundContextInterceptor.ENGINE_VERSION_KEY.get();
  }

  public boolean isPresent() {
    return engineKind() != null && !engineKind().isBlank();
  }
}

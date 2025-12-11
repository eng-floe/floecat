package ai.floedb.floecat.service.context.impl;

import jakarta.enterprise.context.RequestScoped;

@RequestScoped
public class EngineContextProvider {

  public String engineKind() {
    return InboundContextInterceptor.ENGINE_KIND_KEY.get();
  }

  public String engineVersion() {
    return InboundContextInterceptor.ENGINE_VERSION_KEY.get();
  }
}

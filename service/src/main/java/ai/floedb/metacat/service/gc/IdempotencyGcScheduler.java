package ai.floedb.metacat.service.gc;

import ai.floedb.metacat.service.repo.impl.TenantRepository;
import ai.floedb.metacat.tenant.rpc.Tenant;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class IdempotencyGcScheduler {

  @Inject TenantRepository tenants;
  @Inject IdempotencyGc idempotencyGc;
  @Inject Clock clock;

  private final Map<String, String> tokenByTenant = new ConcurrentHashMap<>();

  @ConfigProperty(name = "metacat.gc.idempotency.tenants-page-size", defaultValue = "200")
  int tenantsPageSize;

  @ConfigProperty(name = "metacat.gc.idempotency.max-tick-millis", defaultValue = "4000")
  long maxTickMillis;

  @Produces
  @ApplicationScoped
  public Clock produceClock() {
    return Clock.systemUTC();
  }

  @Scheduled(
      every = "{metacat.gc.idempotency.tick-every}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void tick() {
    final long tickDeadline = clock.millis() + maxTickMillis;

    List<Tenant> allTenants = fetchAllTenants();

    Collections.shuffle(allTenants);

    for (Tenant tenant : allTenants) {
      if (clock.millis() >= tickDeadline) {
        break;
      }

      String tenantId = tenant.getResourceId().getId();
      String currentTenant = tokenByTenant.getOrDefault(tenantId, "");

      var result = idempotencyGc.runSliceForTenant(tenantId, currentTenant);

      if (result.nextToken() == null || result.nextToken().isBlank()) {
        tokenByTenant.remove(tenantId);
      } else {
        tokenByTenant.put(tenantId, result.nextToken());
      }
    }
  }

  private List<Tenant> fetchAllTenants() {
    List<Tenant> tenantsList = new ArrayList<>();
    String token = "";
    StringBuilder next = new StringBuilder();
    do {
      var page = tenants.list(tenantsPageSize, token, next);
      tenantsList.addAll(page);
      token = next.toString();
      next.setLength(0);
    } while (!token.isBlank());
    return tenantsList;
  }
}

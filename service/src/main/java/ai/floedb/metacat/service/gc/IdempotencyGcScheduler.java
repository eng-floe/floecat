package ai.floedb.metacat.service.gc;

import ai.floedb.metacat.service.repo.impl.TenantRepository;
import ai.floedb.metacat.tenant.rpc.Tenant;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.ScheduledExecution;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
public class IdempotencyGcScheduler {

  @Inject Provider<TenantRepository> tenants;
  @Inject Provider<IdempotencyGc> idempotencyGc;
  @Inject MeterRegistry registry;

  private Counter tickCounter;
  private Counter sliceCounter;
  private final AtomicInteger running = new AtomicInteger(0);
  private final AtomicInteger enabledGauge = new AtomicInteger(0);
  private final AtomicLong lastTickStartMs = new AtomicLong(0);
  private final AtomicLong lastTickEndMs = new AtomicLong(0);

  @PostConstruct
  void initMeters() {
    tickCounter =
        Counter.builder("metacat_gc_idempotency_ticks")
            .description("Scheduler ticks")
            .register(registry);
    sliceCounter =
        Counter.builder("metacat_gc_idempotency_slices")
            .description("Per-tenant slices")
            .register(registry);
    registry.gauge("metacat_gc_idempotency_running", running);
    registry.gauge("metacat_gc_idempotency_enabled", enabledGauge);
    registry.gauge("metacat_gc_idempotency_last_tick_start_ms", lastTickStartMs);
    registry.gauge("metacat_gc_idempotency_last_tick_end_ms", lastTickEndMs);
  }

  private final Map<String, String> tokenByTenant = new ConcurrentHashMap<>();
  private volatile boolean stopping;

  void onStop(@Observes ShutdownEvent ev) {
    stopping = true;
    DisabledOrStopping.signalStopping();
  }

  @Scheduled(
      every = "{metacat.gc.idempotency.tick-every}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
      skipExecutionIf = DisabledOrStopping.class)
  void tick() {
    if (stopping) {
      return;
    }

    var cfg = ConfigProvider.getConfig();
    boolean enabled =
        cfg.getOptionalValue("metacat.gc.idempotency.enabled", Boolean.class).orElse(true);
    enabledGauge.set(enabled ? 1 : 0);
    if (!enabled) {
      return;
    }

    final TenantRepository tenantRepo;
    final IdempotencyGc gc;
    try {
      tenantRepo = tenants.get();
      gc = idempotencyGc.get();
    } catch (Throwable ignored) {
      return;
    }

    final long now = System.currentTimeMillis();
    lastTickStartMs.set(now);
    running.set(1);
    tickCounter.increment();

    final long maxTickMillis =
        cfg.getOptionalValue("metacat.gc.idempotency.max-tick-millis", Long.class).orElse(4000L);
    final int tenantsPageSize =
        cfg.getOptionalValue("metacat.gc.idempotency.tenants-page-size", Integer.class).orElse(200);
    final long deadline = now + maxTickMillis;

    try {
      List<Tenant> allTenants = fetchAllTenants(tenantRepo, tenantsPageSize);
      Collections.shuffle(allTenants);

      for (Tenant tenant : allTenants) {
        if (System.currentTimeMillis() >= deadline || stopping) {
          break;
        }

        String tenantId = tenant.getResourceId().getId();
        String token = tokenByTenant.getOrDefault(tenantId, "");

        var result = gc.runSliceForTenant(tenantId, token);
        sliceCounter.increment();

        if (result.nextToken() == null || result.nextToken().isBlank()) {
          tokenByTenant.remove(tenantId);
        } else {
          tokenByTenant.put(tenantId, result.nextToken());
        }
      }
    } finally {
      lastTickEndMs.set(System.currentTimeMillis());
      running.set(0);
    }
  }

  private static List<Tenant> fetchAllTenants(TenantRepository repo, int pageSize) {
    List<Tenant> out = new ArrayList<>();
    String tok = "";
    StringBuilder next = new StringBuilder();
    do {
      var page = repo.list(pageSize, tok, next);
      out.addAll(page);
      tok = next.toString();
      next.setLength(0);
    } while (!tok.isBlank());
    return out;
  }

  public static final class DisabledOrStopping implements Scheduled.SkipPredicate {
    private static volatile boolean stopping;

    static void signalStopping() {
      stopping = true;
    }

    @Override
    public boolean test(ScheduledExecution execution) {
      boolean enabled =
          ConfigProvider.getConfig()
              .getOptionalValue("metacat.gc.idempotency.enabled", Boolean.class)
              .orElse(true);
      return !enabled || stopping;
    }
  }
}

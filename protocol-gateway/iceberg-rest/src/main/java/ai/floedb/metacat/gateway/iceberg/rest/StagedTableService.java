package ai.floedb.metacat.gateway.iceberg.rest;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;

/** Service facade for staged table persistence. */
@ApplicationScoped
public class StagedTableService {
  private static final long MIN_TTL_SECONDS = 60L;

  private final StagedTableRepository repository;
  private final Duration stageTtl;

  @Inject
  public StagedTableService(StagedTableRepository repository, Config config) {
    this.repository = repository;
    long seconds =
        config.getOptionalValue("metacat.gateway.stage-ttl-seconds", Long.class).orElse(900L);
    if (seconds < MIN_TTL_SECONDS) {
      seconds = MIN_TTL_SECONDS;
    }
    this.stageTtl = Duration.ofSeconds(seconds);
  }

  public StagedTableEntry saveStage(StagedTableEntry entry) {
    StagedTableEntry stored = repository.save(entry.initializeTimestamps(Instant.now()));
    expireStages();
    return stored;
  }

  public Optional<StagedTableEntry> getStage(StagedTableKey key) {
    expireStages();
    return repository.get(key);
  }

  public Optional<StagedTableEntry> findLatestStage(
      String tenantId, String catalogName, List<String> namespacePath, String tableName) {
    expireStages();
    return repository.findLatest(tenantId, catalogName, namespacePath, tableName);
  }

  public void deleteStage(StagedTableKey key) {
    repository.delete(key);
  }

  public int expireStages() {
    Instant cutoff = Instant.now().minus(stageTtl);
    return repository.deleteOlderThan(cutoff);
  }

  Duration stageTtl() {
    return stageTtl;
  }
}

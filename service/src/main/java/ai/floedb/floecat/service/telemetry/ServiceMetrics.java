package ai.floedb.floecat.service.telemetry;

import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.MetricType;

public final class ServiceMetrics {

  private ServiceMetrics() {}

  private static final String CONTRACT = "v1";

  public static final class Cache {
    public static final MetricId ENABLED =
        new MetricId("floecat.service.cache.enabled", MetricType.GAUGE, "", CONTRACT, "service");
    public static final MetricId MAX_SIZE =
        new MetricId("floecat.service.cache.max.size", MetricType.GAUGE, "", CONTRACT, "service");
    public static final MetricId ACCOUNTS =
        new MetricId("floecat.service.cache.accounts", MetricType.GAUGE, "", CONTRACT, "service");
  }

  public static final class Storage {
    public static final MetricId ACCOUNT_POINTERS =
        new MetricId(
            "floecat.service.storage.account.pointers", MetricType.GAUGE, "", CONTRACT, "service");
    public static final MetricId ACCOUNT_BYTES =
        new MetricId(
            "floecat.service.storage.account.bytes",
            MetricType.GAUGE,
            "bytes",
            CONTRACT,
            "service");
  }

  public static final class Hint {
    public static final MetricId CACHE_WEIGHT =
        new MetricId(
            "floecat.service.hint.cache.weight", MetricType.GAUGE, "bytes", CONTRACT, "service");
  }
}

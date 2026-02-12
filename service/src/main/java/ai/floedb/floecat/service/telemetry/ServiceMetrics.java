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
    public static final MetricId ENTRIES =
        new MetricId("floecat.service.cache.entries", MetricType.GAUGE, "", CONTRACT, "service");
    public static final MetricId LOAD_LATENCY =
        new MetricId(
            "floecat.service.cache.load.latency", MetricType.TIMER, "seconds", CONTRACT, "service");
  }

  public static final class GC {
    public static final MetricId POINTER_RUNNING =
        new MetricId(
            "floecat.service.gc.pointer.running", MetricType.GAUGE, "", CONTRACT, "service");
    public static final MetricId POINTER_ENABLED =
        new MetricId(
            "floecat.service.gc.pointer.enabled", MetricType.GAUGE, "", CONTRACT, "service");
    public static final MetricId POINTER_LAST_TICK_START =
        new MetricId(
            "floecat.service.gc.pointer.last.tick.start.ms",
            MetricType.GAUGE,
            "milliseconds",
            CONTRACT,
            "service");
    public static final MetricId POINTER_LAST_TICK_END =
        new MetricId(
            "floecat.service.gc.pointer.last.tick.end.ms",
            MetricType.GAUGE,
            "milliseconds",
            CONTRACT,
            "service");

    public static final MetricId CAS_RUNNING =
        new MetricId("floecat.service.gc.cas.running", MetricType.GAUGE, "", CONTRACT, "service");
    public static final MetricId CAS_ENABLED =
        new MetricId("floecat.service.gc.cas.enabled", MetricType.GAUGE, "", CONTRACT, "service");
    public static final MetricId CAS_LAST_TICK_START =
        new MetricId(
            "floecat.service.gc.cas.last.tick.start.ms",
            MetricType.GAUGE,
            "milliseconds",
            CONTRACT,
            "service");
    public static final MetricId CAS_LAST_TICK_END =
        new MetricId(
            "floecat.service.gc.cas.last.tick.end.ms",
            MetricType.GAUGE,
            "milliseconds",
            CONTRACT,
            "service");

    public static final MetricId IDEMP_RUNNING =
        new MetricId(
            "floecat.service.gc.idempotency.running", MetricType.GAUGE, "", CONTRACT, "service");
    public static final MetricId IDEMP_ENABLED =
        new MetricId(
            "floecat.service.gc.idempotency.enabled", MetricType.GAUGE, "", CONTRACT, "service");
    public static final MetricId IDEMP_LAST_TICK_START =
        new MetricId(
            "floecat.service.gc.idempotency.last.tick.start.ms",
            MetricType.GAUGE,
            "milliseconds",
            CONTRACT,
            "service");
    public static final MetricId IDEMP_LAST_TICK_END =
        new MetricId(
            "floecat.service.gc.idempotency.last.tick.end.ms",
            MetricType.GAUGE,
            "milliseconds",
            CONTRACT,
            "service");
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

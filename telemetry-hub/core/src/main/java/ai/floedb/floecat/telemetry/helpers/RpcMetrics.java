package ai.floedb.floecat.telemetry.helpers;

import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.ObservationScope;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Helper that emits RPC metrics with canonical tags. */
public final class RpcMetrics extends BaseMetrics {
  private static final ConcurrentMap<List<Tag>, AtomicInteger> ACTIVE_REQUESTS =
      new ConcurrentHashMap<>();

  private final Observability observability;
  private final List<Tag> canonicalTags;
  private final AtomicInteger activeRequests;

  public RpcMetrics(Observability observability, String component, String operation, Tag... tags) {
    super(component, operation, tags);
    this.observability = observability;
    this.canonicalTags = sortTags(metricTagList());
    this.activeRequests = registerActiveGauge();
  }

  private AtomicInteger registerActiveGauge() {
    return ACTIVE_REQUESTS.computeIfAbsent(
        canonicalTags,
        tags -> {
          AtomicInteger counter = new AtomicInteger();
          observability.gauge(
              Telemetry.Metrics.RPC_ACTIVE,
              counter::get,
              "Active RPC requests",
              tags.toArray(Tag[]::new));
          return counter;
        });
  }

  private static List<Tag> sortTags(List<Tag> tags) {
    if (tags.isEmpty()) {
      return List.of();
    }
    return tags.stream().sorted(Comparator.comparing(Tag::key).thenComparing(Tag::value)).toList();
  }

  public void incrementActiveRequests() {
    activeRequests.incrementAndGet();
  }

  public void decrementActiveRequests() {
    activeRequests.updateAndGet(current -> Math.max(0, current - 1));
  }

  public void recordRequest(String account, String status, Tag... extraTags) {
    List<Tag> dynamic = new ArrayList<>();
    dynamic.add(Tag.of(TagKey.ACCOUNT, account));
    dynamic.add(Tag.of(TagKey.STATUS, status));
    addExtra(dynamic, extraTags);
    observability.counter(Telemetry.Metrics.RPC_REQUESTS, 1, metricTags(dynamic));
  }

  public ObservationScopeHolder observe(Tag... tags) {
    return new ObservationScopeHolder(
        observability.observe(
            Observability.Category.RPC, component(), operation(), scopeTags(tags)));
  }

  /** Wrapper around {@link Observability} scope that decrements the active gauge when closed. */
  public final class ObservationScopeHolder implements ObservationScope {
    private final ObservationScope delegate;
    private boolean closed;

    private ObservationScopeHolder(ObservationScope delegate) {
      this.delegate = delegate;
      incrementActiveRequests();
    }

    @Override
    public void success() {
      delegate.success();
    }

    @Override
    public void error(Throwable throwable) {
      delegate.error(throwable);
    }

    @Override
    public void retry() {
      delegate.retry();
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      delegate.close();
      decrementActiveRequests();
    }

    @Override
    public void status(String status) {
      delegate.status(status);
    }
  }
}

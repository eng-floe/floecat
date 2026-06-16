/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.reconciler.jobs;

import ai.floedb.floecat.connector.rpc.ReconcileMode;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
public class ReconcilerSettingsStore {
  private volatile boolean autoEnabled = true;
  private volatile long defaultIntervalMs = Duration.ofMinutes(10).toMillis();
  private volatile ReconcileMode defaultMode = ReconcileMode.RM_INCREMENTAL;
  private volatile long finishedJobRetentionMs = Duration.ofDays(7).toMillis();

  @PostConstruct
  void init() {
    var cfg = ConfigProvider.getConfig();
    autoEnabled =
        cfg.getOptionalValue("floecat.reconciler.auto.enabled", Boolean.class).orElse(true);
    defaultIntervalMs =
        cfg.getOptionalValue("floecat.reconciler.default-interval", Duration.class)
            .map(Duration::toMillis)
            .orElse(Duration.ofMinutes(10).toMillis());
    if (defaultIntervalMs <= 0L) {
      defaultIntervalMs = Duration.ofMinutes(10).toMillis();
    }
    String configuredMode =
        cfg.getOptionalValue("floecat.reconciler.default-mode", String.class)
            .orElse(ReconcileMode.RM_INCREMENTAL.name());
    try {
      defaultMode = ReconcileMode.valueOf(configuredMode);
    } catch (IllegalArgumentException e) {
      defaultMode = ReconcileMode.RM_INCREMENTAL;
    }
    if (defaultMode == ReconcileMode.RM_UNSPECIFIED) {
      defaultMode = ReconcileMode.RM_INCREMENTAL;
    }
    finishedJobRetentionMs =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.retention-ms", Long.class)
            .orElse(Duration.ofDays(7).toMillis());
    if (finishedJobRetentionMs <= 0L) {
      finishedJobRetentionMs = Duration.ofDays(7).toMillis();
    }
  }

  public boolean isAutoEnabled() {
    return autoEnabled;
  }

  public long defaultIntervalMs() {
    return defaultIntervalMs;
  }

  public ReconcileMode defaultMode() {
    return defaultMode;
  }

  public long finishedJobRetentionMs() {
    return finishedJobRetentionMs;
  }

  public synchronized void update(
      Boolean autoEnabledIn,
      Long defaultIntervalMsIn,
      ReconcileMode defaultModeIn,
      Long finishedJobRetentionMsIn) {
    if (autoEnabledIn != null) {
      this.autoEnabled = autoEnabledIn;
    }
    if (defaultIntervalMsIn != null) {
      if (defaultIntervalMsIn <= 0L) {
        throw new IllegalArgumentException("default_interval must be > 0");
      }
      this.defaultIntervalMs = defaultIntervalMsIn;
    }
    if (defaultModeIn != null) {
      if (defaultModeIn == ReconcileMode.RM_UNSPECIFIED) {
        throw new IllegalArgumentException("default_mode must be set");
      }
      this.defaultMode = defaultModeIn;
    }
    if (finishedJobRetentionMsIn != null) {
      if (finishedJobRetentionMsIn <= 0L) {
        throw new IllegalArgumentException("finished_job_retention must be > 0");
      }
      this.finishedJobRetentionMs = finishedJobRetentionMsIn;
    }
  }
}

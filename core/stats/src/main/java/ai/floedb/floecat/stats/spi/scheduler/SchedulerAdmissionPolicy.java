package ai.floedb.floecat.stats.spi.scheduler;

import ai.floedb.floecat.stats.spi.SchedulerHealthBand;

public interface SchedulerAdmissionPolicy {
  AdmissionDecision decide(
      SchedulerPriorityPolicy.PriorityAssignment assignment, SchedulerHealthBand currentBand);

  enum AdmissionDecision {
    ADMIT,
    DEFER,
    REJECT
  }
}

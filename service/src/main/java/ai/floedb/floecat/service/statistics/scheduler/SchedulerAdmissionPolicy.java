package ai.floedb.floecat.service.statistics.scheduler;

import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;

public interface SchedulerAdmissionPolicy {
  AdmissionDecision decide(
      SchedulerPriorityPolicy.PriorityAssignment assignment, SchedulerHealthBand currentBand);

  enum AdmissionDecision {
    ADMIT,
    DEFER,
    REJECT
  }
}

package ai.floedb.floecat.service.statistics.scheduler;

import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import java.util.List;
import java.util.Optional;

public interface SchedulerPreemptionPolicy {
  Optional<String> selectVictim(
      String incomingJobId, List<RunningJobInfo> candidates, SchedulerContext context);

  record RunningJobInfo(
      String jobId,
      StatsPriorityClass priorityClass,
      long startedAtMs,
      int completedFiles,
      int totalFiles) {}
}

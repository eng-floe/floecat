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

package ai.floedb.floecat.service.statistics.scheduler;

import jakarta.inject.Qualifier;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * CDI qualifier that identifies a named scheduler profile.
 *
 * <p>Beans implementing {@link SchedulerPriorityPolicy}, {@link SchedulerAdmissionPolicy}, or
 * {@link SchedulerPreemptionPolicy} must carry this annotation with the profile {@link #name()} so
 * that {@link SchedulerPolicyRegistry} can resolve them at startup. The active profile is selected
 * by the {@code floecat.stats.scheduler.profile} configuration key (default: {@code "default"}).
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * @ApplicationScoped
 * @SchedulerProfile(name = "default")
 * public class DefaultSchedulerPriorityPolicy implements SchedulerPriorityPolicy { ... }
 * }</pre>
 */
@Qualifier
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
public @interface SchedulerProfile {

  /** Logical name of the scheduler profile, e.g. {@code "default"} or {@code "latency-first"}. */
  String name();
}

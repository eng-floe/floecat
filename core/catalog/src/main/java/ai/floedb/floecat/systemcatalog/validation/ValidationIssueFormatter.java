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

package ai.floedb.floecat.systemcatalog.validation;

import ai.floedb.floecat.systemcatalog.engine.VersionIntervals;

public final class ValidationIssueFormatter {
  private ValidationIssueFormatter() {}

  public static String format(ValidationIssue issue) {
    StringBuilder b = new StringBuilder();
    b.append(issue.severity()).append(':').append(issue.code()).append(':').append(issue.ctx());
    if (issue.interval() != null) {
      b.append('@').append(formatInterval(issue.interval()));
    }
    for (String arg : issue.args()) {
      b.append(':').append(arg);
    }
    return b.toString();
  }

  static String formatInterval(VersionIntervals.VersionInterval interval) {
    if (interval == null) {
      return "<none>";
    }
    return "[" + formatBound(interval.min()) + "," + formatBound(interval.max()) + "]";
  }

  private static String formatBound(VersionIntervals.VersionBound bound) {
    if (bound == null) {
      return "<none>";
    }
    if (bound.isNegInf()) {
      return "-∞";
    }
    if (bound.isPosInf()) {
      return "+∞";
    }
    return bound.value();
  }
}

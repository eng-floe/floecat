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

import java.util.List;
import java.util.stream.Collectors;

/** Shared helpers for converting validation issues into fail-fast exceptions. */
public final class ValidationFailures {

  private ValidationFailures() {}

  public static void throwOnErrorIssues(String context, List<ValidationIssue> issues) {
    List<ValidationIssue> errors =
        issues == null
            ? List.of()
            : issues.stream().filter(issue -> issue.severity() == Severity.ERROR).toList();
    if (errors.isEmpty()) {
      return;
    }
    String details =
        errors.stream()
            .limit(5)
            .map(ValidationIssueFormatter::format)
            .collect(Collectors.joining("; "));
    throw new IllegalStateException(
        context + " (" + errors.size() + " error(s))" + (details.isEmpty() ? "" : ": " + details));
  }
}

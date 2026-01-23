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

package ai.floedb.floecat.extensions.floedb.validation;

import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.List;
import java.util.Set;

record TypeValidationResult(
    Lookup lookup, IntervalIndex<Integer> typeIntervals, List<ValidationIssue> errors) {}

record FunctionValidationResult(
    Set<Integer> functionOids,
    IntervalIndex<Integer> functionIntervals,
    List<ValidationIssue> errors) {}

record OperatorValidationResult(
    Set<Integer> operatorOids,
    IntervalIndex<Integer> operatorIntervals,
    List<ValidationIssue> errors) {}

record CollationValidationResult(
    Set<Integer> collationOids,
    IntervalIndex<Integer> collationIntervals,
    List<ValidationIssue> errors) {}

record SimpleValidationResult(List<ValidationIssue> errors) {}

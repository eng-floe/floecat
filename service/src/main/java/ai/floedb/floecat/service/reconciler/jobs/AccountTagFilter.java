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

import java.util.Map;

/**
 * Startup tag rule deciding whether the reconcile planner manages a given account.
 *
 * <p>The rule is chosen once at startup and never changes as accounts come and go — accounts are
 * created already carrying (or not carrying) the tag the rule keys on. Two selection modes, plus a
 * pass-all default:
 *
 * <ul>
 *   <li>{@code ignore-key:<key>} — skip any account carrying {@code <key>}, whatever its value.
 *       Used by the steady-state floecat to ignore every CI account, past or present, without a
 *       restart as runs come and go.
 *   <li>{@code own-value:<key>=<value>} — manage only the account whose {@code <key>} tag equals
 *       {@code <value>}. Used by an overlay floecat to own exactly this run's account and never
 *       grab a prior run's torn-down account.
 * </ul>
 *
 * <p>An unset/blank spec yields {@link PassAll}, preserving the account-blind steady-state
 * behavior.
 */
public sealed interface AccountTagFilter {

  String IGNORE_KEY_PREFIX = "ignore-key:";
  String OWN_VALUE_PREFIX = "own-value:";

  /** Whether the planner should manage an account carrying {@code tags}. */
  boolean accountPasses(Map<String, String> tags);

  /** Manage every account (no filtering). */
  record PassAll() implements AccountTagFilter {
    @Override
    public boolean accountPasses(Map<String, String> tags) {
      return true;
    }
  }

  /** Skip any account carrying {@code key}, regardless of value. */
  record IgnoreKey(String key) implements AccountTagFilter {
    @Override
    public boolean accountPasses(Map<String, String> tags) {
      return !tags.containsKey(key);
    }
  }

  /** Manage only the account whose {@code key} tag equals {@code value}. */
  record OwnValue(String key, String value) implements AccountTagFilter {
    @Override
    public boolean accountPasses(Map<String, String> tags) {
      return value.equals(tags.get(key));
    }
  }

  /**
   * Parse a filter spec into a rule. A blank or {@code null} spec yields {@link PassAll}; otherwise
   * the spec must be {@code ignore-key:<key>} or {@code own-value:<key>=<value>}.
   *
   * @throws IllegalArgumentException if the spec is non-blank but not a recognised, well-formed
   *     rule
   */
  static AccountTagFilter parse(String spec) {
    if (spec == null || spec.isBlank()) {
      return new PassAll();
    }
    String s = spec.strip();
    if (s.startsWith(IGNORE_KEY_PREFIX)) {
      String key = s.substring(IGNORE_KEY_PREFIX.length()).strip();
      if (key.isEmpty()) {
        throw new IllegalArgumentException(
            "account-tag-filter '" + spec + "' must be " + IGNORE_KEY_PREFIX + "<key>");
      }
      return new IgnoreKey(key);
    }
    if (s.startsWith(OWN_VALUE_PREFIX)) {
      String kv = s.substring(OWN_VALUE_PREFIX.length());
      int eq = kv.indexOf('=');
      String key = eq < 0 ? "" : kv.substring(0, eq).strip();
      String value = eq < 0 ? "" : kv.substring(eq + 1).strip();
      if (key.isEmpty() || value.isEmpty()) {
        throw new IllegalArgumentException(
            "account-tag-filter '" + spec + "' must be " + OWN_VALUE_PREFIX + "<key>=<value>");
      }
      return new OwnValue(key, value);
    }
    throw new IllegalArgumentException(
        "account-tag-filter '"
            + spec
            + "' must be blank, "
            + IGNORE_KEY_PREFIX
            + "<key>, or "
            + OWN_VALUE_PREFIX
            + "<key>=<value>");
  }
}

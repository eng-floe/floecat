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

package ai.floedb.floecat.catalog.access;

import java.util.Locale;

/** Supported Iceberg REST access-delegation modes. */
public enum IcebergRestAccessDelegationMode {
  VENDED_CREDENTIALS("vended-credentials"),
  NONE("none");

  public static final String PROPERTY = "access-delegation-mode";

  private final String propertyValue;

  IcebergRestAccessDelegationMode(String propertyValue) {
    this.propertyValue = propertyValue;
  }

  public String propertyValue() {
    return propertyValue;
  }

  public boolean requestsVendedCredentials() {
    return this == VENDED_CREDENTIALS;
  }

  /** Parses a configured mode, defaulting an absent value to credential vending. */
  public static IcebergRestAccessDelegationMode parse(String value) {
    if (value == null) {
      return VENDED_CREDENTIALS;
    }
    String normalized = value.trim().toLowerCase(Locale.ROOT);
    for (IcebergRestAccessDelegationMode mode : values()) {
      if (mode.propertyValue.equals(normalized)) {
        return mode;
      }
    }
    throw new IllegalArgumentException("Unsupported " + PROPERTY + ": " + value);
  }
}

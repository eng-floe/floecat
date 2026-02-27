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

package ai.floedb.floecat.systemcatalog.spi.decorator;

import ai.floedb.floecat.query.rpc.ColumnFailureCode;
import java.util.Map;

/** Structured runtime error raised by decorators when metadata generation fails. */
public final class DecorationException extends RuntimeException {

  private final ColumnFailureCode code;
  private final Integer extensionCodeValue;
  private final Map<String, String> details;

  public DecorationException(ColumnFailureCode code, String message) {
    this(code, message, null, Map.of());
  }

  public DecorationException(ColumnFailureCode code, String message, Throwable cause) {
    this(code, message, cause, Map.of());
  }

  public DecorationException(
      ColumnFailureCode code, String message, Throwable cause, Map<String, String> details) {
    super(message, cause);
    boolean hasCoreCode = code != null && code != ColumnFailureCode.COLUMN_FAILURE_CODE_UNSPECIFIED;
    if (!hasCoreCode) {
      throw new IllegalArgumentException("A non-unspecified core code must be provided");
    }
    this.code = code;
    this.extensionCodeValue = null;
    this.details = details == null ? Map.of() : Map.copyOf(details);
  }

  /** Constructor for extension-defined numeric failure codes. */
  public DecorationException(int extensionCodeValue, String message) {
    this(extensionCodeValue, message, null, Map.of());
  }

  /** Constructor for extension-defined numeric failure codes. */
  public DecorationException(int extensionCodeValue, String message, Throwable cause) {
    this(extensionCodeValue, message, cause, Map.of());
  }

  public DecorationException(
      int extensionCodeValue, String message, Throwable cause, Map<String, String> details) {
    super(message, cause);
    if (extensionCodeValue <= 0) {
      throw new IllegalArgumentException("A positive extension code must be provided");
    }
    this.code = ColumnFailureCode.COLUMN_FAILURE_CODE_ENGINE_EXTENSION;
    this.extensionCodeValue = extensionCodeValue;
    this.details = details == null ? Map.of() : Map.copyOf(details);
  }

  public ColumnFailureCode code() {
    return code;
  }

  public Integer extensionCodeValue() {
    return extensionCodeValue;
  }

  public boolean hasExtensionCodeValue() {
    return extensionCodeValue != null;
  }

  public Map<String, String> details() {
    return details;
  }
}

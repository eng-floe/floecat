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

package ai.floedb.floecat.storage.aws;

public final class ClosedAwsClientDetector {

  static final String CONNECTION_POOL_SHUT_DOWN = "Connection pool shut down";

  private ClosedAwsClientDetector() {}

  public static boolean isConnectionPoolShutdown(Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      String message = current.getMessage();
      if (message != null && message.contains(CONNECTION_POOL_SHUT_DOWN)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }
}

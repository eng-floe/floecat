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

package ai.floedb.floecat.service.util;

import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class TestDataResetter {
  @Inject PointerStore ptr;
  @Inject BlobStore blobs;

  private static final String GLOBAL_ACCOUNTS_BY_ID_PREFIX = "/accounts/by-id/";
  private static final String GLOBAL_ACCOUNTS_BY_NAME_PREFIX = "/accounts/by-name/";

  public void wipeAll() {
    if (false) ptr.dump("BEFORE WIPE");
    var accountIds = listAccountIds();
    var accountNames = listAccountNames();

    for (var tid : accountIds) {
      ptr.deleteByPrefix("/accounts/" + tid + "/");
      ptr.deleteByPrefix(GLOBAL_ACCOUNTS_BY_ID_PREFIX + tid);
      ptr.delete(GLOBAL_ACCOUNTS_BY_ID_PREFIX + tid);
    }
    for (var nm : accountNames) {
      ptr.delete(GLOBAL_ACCOUNTS_BY_NAME_PREFIX + nm);
    }
    ptr.deleteByPrefix("/accounts/");

    for (var tid : accountIds) {
      blobs.deletePrefix("/accounts/" + tid + "/");
    }
    blobs.deletePrefix("/accounts/");

    if (!ptr.isEmpty()) {
      ptr.dump("AFTER WIPE, NON-EMPTY");
    }
  }

  private List<String> listAccountIds() {
    var ids = new ArrayList<String>();
    String token = "";
    do {
      var next = new StringBuilder();
      var rows = ptr.listPointersByPrefix(GLOBAL_ACCOUNTS_BY_ID_PREFIX, 200, token, next);
      for (var r : rows) {
        var k =
            r.getKey()
                .replace(GLOBAL_ACCOUNTS_BY_ID_PREFIX, "")
                .replace(GLOBAL_ACCOUNTS_BY_ID_PREFIX.substring(1), "");
        var parts = k.split("/");
        if (parts.length > 0) {
          ids.add(parts[0]);
        }
      }
      token = next.toString();
    } while (!token.isBlank());
    return ids;
  }

  private List<String> listAccountNames() {
    var names = new ArrayList<String>();
    String token = "";
    do {
      var next = new StringBuilder();
      var rows = ptr.listPointersByPrefix(GLOBAL_ACCOUNTS_BY_NAME_PREFIX, 200, token, next);
      for (var r : rows) {
        var k =
            r.getKey()
                .replace(GLOBAL_ACCOUNTS_BY_NAME_PREFIX, "")
                .replace(GLOBAL_ACCOUNTS_BY_NAME_PREFIX.substring(1), "");
        var parts = k.split("/");
        if (parts.length > 0) {
          names.add(parts[0]);
        }
      }
      token = next.toString();
    } while (!token.isBlank());
    return names;
  }
}

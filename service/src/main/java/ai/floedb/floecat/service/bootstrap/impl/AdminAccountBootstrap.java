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

package ai.floedb.floecat.service.bootstrap.impl;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.common.AccountIds;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import com.google.protobuf.util.Timestamps;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class AdminAccountBootstrap {
  private static final Logger LOG = Logger.getLogger(AdminAccountBootstrap.class);

  @Inject AccountRepository accounts;
  @Inject Vertx vertx;

  @ConfigProperty(name = "floecat.auth.mode", defaultValue = "auto")
  String authMode;

  @ConfigProperty(name = "floecat.auth.admin.account", defaultValue = "admin")
  String adminAccountName;

  @ConfigProperty(
      name = "floecat.auth.admin.account.description",
      defaultValue = "Bootstrap admin account")
  String adminAccountDescription;

  void onStart(@Observes StartupEvent ev) {
    if (!"oidc".equalsIgnoreCase(authMode)) {
      LOG.infof("Admin account bootstrap skipped (auth.mode=%s)", authMode);
      return;
    }
    if (adminAccountName == null || adminAccountName.isBlank()) {
      LOG.warn("Admin account bootstrap skipped (admin account name is blank)");
      return;
    }
    vertx
        .<Void>executeBlocking(
            promise -> {
              try {
                ensureAdminAccount();
                promise.complete(null);
              } catch (Throwable t) {
                promise.fail(t);
              }
            },
            true)
        .onSuccess(v -> LOG.info("Admin account bootstrap completed"))
        .onFailure(t -> LOG.error("Admin account bootstrap failed", t));
  }

  private void ensureAdminAccount() {
    var existing = accounts.getByName(adminAccountName).orElse(null);
    if (existing != null) {
      LOG.infof(
          "Admin account already exists: %s (%s)",
          existing.getDisplayName(), existing.getResourceId().getId());
      return;
    }

    String id = AccountIds.deterministicAccountId(adminAccountName);
    var rid =
        ResourceId.newBuilder().setAccountId(id).setId(id).setKind(ResourceKind.RK_ACCOUNT).build();
    var account =
        Account.newBuilder()
            .setResourceId(rid)
            .setDisplayName(adminAccountName)
            .setDescription(adminAccountDescription)
            .setCreatedAt(Timestamps.fromMillis(System.currentTimeMillis()))
            .build();
    accounts.create(account);
    LOG.infof("Admin account created: %s (%s)", adminAccountName, id);
  }
}

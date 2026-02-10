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

package ai.floedb.floecat.service.transaction.impl;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.impl.TransactionIntentRepository;
import ai.floedb.floecat.service.repo.impl.TransactionRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionIntentApplier {

  private static final Logger LOG = Logger.getLogger(TransactionIntentApplier.class);

  @Inject ai.floedb.floecat.storage.spi.PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject TransactionRepository txRepo;
  @Inject TransactionIntentRepository intentRepo;
  @Inject TransactionIntentApplierSupport applierSupport;

  public record Result(int scanned, int applied, int skipped) {}

  public Result runForAccount(String accountId, long deadlineMs) {
    int pageSize =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.transaction.apply.page-size", Integer.class)
            .orElse(500);

    String prefix = Keys.transactionIntentPointerByTargetPrefix(accountId);
    String token = "";
    StringBuilder next = new StringBuilder();
    int scanned = 0;
    int applied = 0;
    int skipped = 0;

    do {
      if (System.currentTimeMillis() > deadlineMs) {
        break;
      }
      List<Pointer> rows = pointerStore.listPointersByPrefix(prefix, pageSize, token, next);
      for (Pointer ptr : rows) {
        scanned++;
        TransactionIntent intent = readIntent(ptr.getBlobUri());
        if (intent == null) {
          skipped++;
          continue;
        }
        Transaction txn = txRepo.getById(accountId, intent.getTxId()).orElse(null);
        if (txn == null || txn.getState() != TransactionState.TS_COMMITTED) {
          skipped++;
          continue;
        }
        if (applierSupport.applyIntentBestEffort(intent, intentRepo)) {
          applied++;
        } else {
          skipped++;
        }
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());

    return new Result(scanned, applied, skipped);
  }

  private TransactionIntent readIntent(String blobUri) {
    try {
      byte[] bytes = blobStore.get(blobUri);
      if (bytes == null) {
        LOG.debugf("intent blob missing: %s", blobUri);
        return null;
      }
      return TransactionIntent.parseFrom(bytes);
    } catch (Exception e) {
      LOG.debugf("intent blob parse failed: %s", blobUri, e);
      return null;
    }
  }
}

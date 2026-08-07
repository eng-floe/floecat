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

package ai.floedb.floecat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The residual sweep account teardown runs after its per-resource deletes, to clear index rows a
 * delete could not remove because the blob naming them was unreadable.
 *
 * <p>What it must not clear is anything nested under a catalog. A catalog's key space contains the
 * namespace by-path rows and the table, view and relation by-name rows of everything inside it, and
 * those rows are the only handles the recursive drop — and a retried DeleteAccount — have for
 * reaching those resources. A teardown that has not finished still needs them.
 */
class ResidualRowSweepTest {

  private static final String ACCOUNT = "acct";
  private static final String CATALOG = "cat";
  private static final String NAMESPACE = "ns";

  @Test
  void catalogSweepClearsCatalogIndexRowsOnly() {
    var pointers = new InMemoryPointerStore();
    var repo = new CatalogRepository(pointers, new InMemoryBlobStore());

    put(pointers, Keys.catalogPointerById(ACCOUNT, CATALOG));
    put(pointers, Keys.catalogPointerByName(ACCOUNT, "sales"));
    // Nested under the catalog, and owned by the recursive drop rather than by this sweep.
    String namespaceByPath = Keys.namespacePointerByPath(ACCOUNT, CATALOG, List.of("db", "orders"));
    String tableByName = Keys.tablePointerByName(ACCOUNT, CATALOG, NAMESPACE, "orders");
    String viewByName = Keys.viewPointerByName(ACCOUNT, CATALOG, NAMESPACE, "orders_v");
    String relationClaim = Keys.relationPointerByName(ACCOUNT, CATALOG, NAMESPACE, "orders");
    put(pointers, namespaceByPath);
    put(pointers, tableByName);
    put(pointers, viewByName);
    put(pointers, relationClaim);

    assertEquals(
        2, repo.deleteResidualRows(ACCOUNT, BatchGuard.NONE), "only the two catalog index rows");

    assertFalse(pointers.get(Keys.catalogPointerById(ACCOUNT, CATALOG)).isPresent());
    assertFalse(pointers.get(Keys.catalogPointerByName(ACCOUNT, "sales")).isPresent());
    assertTrue(
        pointers.get(namespaceByPath).isPresent(),
        "a namespace's by-path row is how teardown finds it again");
    assertTrue(pointers.get(tableByName).isPresent(), "a table's by-name row must survive");
    assertTrue(pointers.get(viewByName).isPresent(), "a view's by-name row must survive");
    assertTrue(pointers.get(relationClaim).isPresent(), "the relation claim must survive");
  }

  @Test
  void connectorSweepClearsConnectorIndexRowsOnly() {
    var pointers = new InMemoryPointerStore();
    var repo = new ConnectorRepository(pointers, new InMemoryBlobStore());

    put(pointers, Keys.connectorPointerById(ACCOUNT, "conn"));
    put(pointers, Keys.connectorPointerByName(ACCOUNT, "warehouse"));
    String otherAccount = Keys.connectorPointerById("other", "conn");
    put(pointers, otherAccount);

    assertEquals(2, repo.deleteResidualRows(ACCOUNT, BatchGuard.NONE));
    assertTrue(pointers.get(otherAccount).isPresent(), "another account's rows are not ours");
  }

  @Test
  void residualSweepRetriesANonCommittingTransactionConflict() {
    var backing = new InMemoryPointerStore();
    String byId = Keys.connectorPointerById(ACCOUNT, "conn");
    String byName = Keys.connectorPointerByName(ACCOUNT, "warehouse");
    put(backing, byId);
    put(backing, byName);
    var pointers =
        new RepoTestPointerStores.DelegatingPointerStore(backing) {
          private boolean conflicted;

          @Override
          public boolean compareAndSetBatch(List<CasOp> ops) {
            if (!conflicted) {
              conflicted = true;
              return false;
            }
            return super.compareAndSetBatch(ops);
          }
        };

    var repo = new ConnectorRepository(pointers, new InMemoryBlobStore());
    assertEquals(2, repo.deleteResidualRows(ACCOUNT, BatchGuard.NONE));
    assertFalse(backing.get(byId).isPresent());
    assertFalse(backing.get(byName).isPresent());
  }

  @Test
  void guardBreakOnSecondIndexFamilyReportsTheEarlierDelete() {
    var backing = new InMemoryPointerStore();
    String byId = Keys.connectorPointerById(ACCOUNT, "conn");
    String byName = Keys.connectorPointerByName(ACCOUNT, "warehouse");
    String accountPointer = Keys.accountPointerById(ACCOUNT);
    put(backing, byId);
    put(backing, byName);
    var pointers =
        new RepoTestPointerStores.DelegatingPointerStore(backing) {
          private boolean reappeared;

          @Override
          public boolean compareAndSetBatch(List<CasOp> ops) {
            if (!reappeared
                && ops.stream()
                    .anyMatch(
                        op -> op instanceof CasDelete delete && delete.key().equals(byName))) {
              reappeared = true;
              put(backing, accountPointer);
            }
            return super.compareAndSetBatch(ops);
          }
        };
    BatchGuard accountGone = absentGuard(pointers, accountPointer);
    var repo = new ConnectorRepository(pointers, new InMemoryBlobStore());

    assertThrows(
        BaseResourceRepository.BatchGuardFailedAfterWriteException.class,
        () -> repo.deleteResidualRows(ACCOUNT, accountGone));
    assertFalse(backing.get(byId).isPresent(), "the first family committed before the guard broke");
    assertTrue(backing.get(byName).isPresent(), "the replacement account's row must survive");
  }

  @Test
  void guardedResidualSweepDoesNotDeleteAfterAccountReappears() {
    var backing = new InMemoryPointerStore();
    String residual = Keys.connectorPointerById(ACCOUNT, "conn");
    String accountPointer = Keys.accountPointerById(ACCOUNT);
    put(backing, residual);

    var pointers =
        new RepoTestPointerStores.DelegatingPointerStore(backing) {
          private boolean reappeared;

          @Override
          public boolean compareAndSetBatch(List<CasOp> ops) {
            if (!reappeared
                && ops.stream()
                    .anyMatch(
                        op -> op instanceof CasDelete delete && delete.key().equals(residual))) {
              reappeared = true;
              put(backing, accountPointer);
            }
            return super.compareAndSetBatch(ops);
          }
        };
    BatchGuard accountGone =
        new BatchGuard() {
          @Override
          public List<PointerStore.CasOp> ops() {
            return List.of(new PointerStore.CasCheckAbsent(accountPointer));
          }

          @Override
          public Outcome reevaluate() {
            return pointers.get(accountPointer).isPresent() ? Outcome.BROKEN : Outcome.HOLDS;
          }

          @Override
          public String describe() {
            return "account " + ACCOUNT;
          }
        };
    var repo = new ConnectorRepository(pointers, new InMemoryBlobStore());

    assertThrows(
        BaseResourceRepository.BatchGuardFailedException.class,
        () -> repo.deleteResidualRows(ACCOUNT, accountGone));
    assertTrue(backing.get(residual).isPresent(), "the recreated account's row must survive");
  }

  private static void put(InMemoryPointerStore pointers, String key) {
    pointers.compareAndSet(key, 0L, Pointer.newBuilder().setKey(key).setVersion(1L).build());
  }

  private static BatchGuard absentGuard(PointerStore pointers, String key) {
    return new BatchGuard() {
      @Override
      public List<PointerStore.CasOp> ops() {
        return List.of(new PointerStore.CasCheckAbsent(key));
      }

      @Override
      public Outcome reevaluate() {
        return pointers.get(key).isPresent() ? Outcome.BROKEN : Outcome.HOLDS;
      }

      @Override
      public String describe() {
        return key;
      }
    };
  }
}

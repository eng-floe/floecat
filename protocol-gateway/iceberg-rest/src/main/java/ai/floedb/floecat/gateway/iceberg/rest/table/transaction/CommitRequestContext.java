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

package ai.floedb.floecat.gateway.iceberg.rest.table.transaction;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import java.util.List;

record CommitRequestContext(
    String accountId,
    String txId,
    String prefix,
    String catalogName,
    ResourceId catalogId,
    String idempotencyBase,
    String requestHash,
    long txCreatedAtMs,
    TransactionState currentState,
    TableGatewaySupport tableSupport,
    List<TransactionCommitRequest.TableChange> changes,
    boolean preMaterializeAssertCreate) {}

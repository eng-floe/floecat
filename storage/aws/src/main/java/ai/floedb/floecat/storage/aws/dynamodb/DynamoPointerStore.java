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

package ai.floedb.floecat.storage.aws.dynamodb;

import ai.floedb.floecat.storage.kv.dynamodb.ps.KvPointerStore;
import ai.floedb.floecat.storage.kv.dynamodb.ps.PointerStoreEntity;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "dynamodb")
public final class DynamoPointerStore extends KvPointerStore {

  @Inject
  public DynamoPointerStore(PointerStoreEntity pointers) {
    super(pointers);
  }
}

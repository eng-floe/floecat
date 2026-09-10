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
package ai.floedb.floecat.client.sharing;

import ai.floedb.floecat.client.sharing.DeltaSharingModel.Schema;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.Share;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.Table;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TableDescription;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TemporaryCredentials;
import java.util.List;

/**
 * The Delta Sharing recipient surface, separated from transport.
 *
 * <p>Only the operations a recipient needs to discover and read shared tables through directory
 * access appear here. The query endpoint, which serves presigned per-file URLs, is deliberately
 * absent: url mode is a different execution model rather than another credential shape, and nothing
 * above this package can represent it yet.
 *
 * <p>Every method raises {@link DeltaSharingException} with a classified {@code Failure}. Paging is
 * the implementation's business; these return complete lists.
 */
public interface DeltaSharingClient extends AutoCloseable {

  /** Every share the recipient token can see. */
  List<Share> listShares();

  /** Every schema in one share. */
  List<Schema> listSchemas(String share);

  /** Every table in one schema, each carrying its advertised access modes. */
  List<Table> listTables(String share, String schema);

  /**
   * The Protocol and Metadata actions for one table.
   *
   * <p>Raises {@code INVALID_RESPONSE} when the server sends fewer than the two required NDJSON
   * lines, which is the shape a proxy error page takes when it arrives with a success status.
   */
  TableDescription describeTable(String share, String schema, String table);

  /**
   * Temporary credentials for reading a table location directly.
   *
   * <p>{@code location} selects an auxiliary location; when absent the server answers for the
   * table's own location. Raises {@code INVALID_REQUEST} for a table that does not advertise
   * directory access, since the endpoint is only required to exist for those.
   *
   * <p>Nothing passes {@code location} yet, and nothing can: selecting among several locations is
   * the only reason to name one, and a table reporting auxiliary locations is refused at the load
   * and at the vend. It is here because the request field is part of the protocol operation this
   * method is, and it is exercised by a test rather than by a caller. Said plainly so the next
   * reader does not have to derive it from the call sites, as this one had to.
   */
  TemporaryCredentials temporaryTableCredentials(
      String share, String schema, String table, String location);

  @Override
  void close();
}

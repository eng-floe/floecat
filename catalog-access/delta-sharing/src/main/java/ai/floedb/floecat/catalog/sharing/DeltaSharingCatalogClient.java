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
package ai.floedb.floecat.catalog.sharing;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogCapabilities;
import ai.floedb.floecat.catalog.access.CatalogCapability;
import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.catalog.access.CatalogTable;
import ai.floedb.floecat.catalog.access.CatalogView;
import ai.floedb.floecat.catalog.access.ExternalObjectIdentity;
import ai.floedb.floecat.catalog.access.NamespacePath;
import ai.floedb.floecat.catalog.access.StorageLocations;
import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import ai.floedb.floecat.catalog.delta.DeltaLogStorageProbe;
import ai.floedb.floecat.client.sharing.DeltaSharingClient;
import ai.floedb.floecat.client.sharing.DeltaSharingException;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.AccessMode;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.Schema;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.Share;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.Table;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TableDescription;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TemporaryCredentials;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A Delta Sharing recipient behind the catalog-access SPI.
 *
 * <p>The sharing hierarchy is share, then schema, then table, which maps onto {@link NamespacePath}
 * without translation: an empty parent lists shares as one-segment paths and a one-segment parent
 * lists schemas as two.
 *
 * <p>Directory access only. A table advertising url alone is discoverable and describable, and
 * refused at the vend by name rather than returning nothing, because a caller that got an empty
 * answer would report a missing storage authority for a table whose provider simply does not
 * delegate storage.
 */
public final class DeltaSharingCatalogClient implements CatalogClient {

  private static final CatalogCapabilities CAPABILITIES =
      CatalogCapabilities.of(
          CatalogCapability.VALIDATE,
          CatalogCapability.LIST_NAMESPACES,
          CatalogCapability.LIST_TABLES,
          CatalogCapability.LOAD_TABLE,
          CatalogCapability.VEND_STORAGE_CREDENTIALS,
          CatalogCapability.VALIDATE_STORAGE_ACCESS,
          CatalogCapability.STABLE_OBJECT_IDS);

  /** Names the property in the refusal it produces, so an operator can find what caused it. */
  static final String STRICT_ACCESS_MODES = "delta.sharing.strict-access-modes";

  /**
   * What {@code delta.sharing.access-modes} reads when the server stated nothing.
   *
   * <p>Not {@code url}. That is the protocol's reading of an absent field, and it is the reading
   * this client deliberately does not apply before vending, so writing it onto the reconciled table
   * would report a fact the share never communicated.
   */
  static final String UNSTATED_ACCESS_MODES = "unstated";

  private final DeltaSharingClient client;
  private final Map<String, String> storageProperties;
  private final boolean strictAccessModes;
  private final DeltaLogStorageProbe storageProbe;
  private final Map<Schema, List<Table>> listedTables =
      new java.util.concurrent.ConcurrentHashMap<>();

  public DeltaSharingCatalogClient(
      DeltaSharingClient client, Map<String, String> storageProperties) {
    this(client, storageProperties, false);
  }

  /**
   * @param strictAccessModes whether a table that states no access modes is read as url only. The
   *     protocol says it is, and the reference server implements the credential endpoint while
   *     stating nothing, so the default asks and lets the server answer.
   */
  public DeltaSharingCatalogClient(
      DeltaSharingClient client, Map<String, String> storageProperties, boolean strictAccessModes) {
    this(client, storageProperties, strictAccessModes, DeltaLogStorageProbe.s3("Delta Sharing"));
  }

  DeltaSharingCatalogClient(
      DeltaSharingClient client,
      Map<String, String> storageProperties,
      boolean strictAccessModes,
      DeltaLogStorageProbe storageProbe) {
    this.client = Objects.requireNonNull(client, "client");
    this.storageProperties = storageProperties == null ? Map.of() : Map.copyOf(storageProperties);
    this.strictAccessModes = strictAccessModes;
    this.storageProbe = Objects.requireNonNull(storageProbe, "storageProbe");
  }

  /** Whether a table stating no access modes is read as url only rather than asked about. */
  boolean strictAccessModes() {
    return strictAccessModes;
  }

  /** The storage properties published alongside every vended credential. */
  Map<String, String> storageProperties() {
    return storageProperties;
  }

  @Override
  public CatalogCapabilities capabilities() {
    return CAPABILITIES;
  }

  @Override
  public void validate() {
    // Listing shares is the smallest call that proves the endpoint is a sharing server and that the
    // recipient token is accepted. It answers with an empty list for a recipient granted nothing,
    // which is a valid configuration rather than a failure.
    translate("validation", client::listShares);
  }

  @Override
  public List<NamespacePath> listNamespaces(NamespacePath parent) {
    List<String> segments = parent == null ? List.of() : parent.segments();
    if (segments.isEmpty()) {
      List<NamespacePath> shares = new ArrayList<>();
      for (Share share : translate("listing shares", client::listShares)) {
        shares.add(new NamespacePath(List.of(share.name())));
      }
      return List.copyOf(shares);
    }
    if (segments.size() == 1) {
      String share = segments.get(0);
      List<NamespacePath> schemas = new ArrayList<>();
      for (Schema schema :
          translate("listing schemas of " + share, () -> client.listSchemas(share))) {
        schemas.add(new NamespacePath(List.of(share, schema.name())));
      }
      return List.copyOf(schemas);
    }
    // Share and schema are the only levels the protocol defines. Deeper is not an error, it is
    // simply empty, which is what a caller walking a tree expects at a leaf.
    return List.of();
  }

  @Override
  public List<CatalogObjectName> listTables(NamespacePath namespace) {
    // Tables live at share.schema and nowhere else. The root and a share are real levels of the
    // hierarchy that simply hold none, and the reconciler lists tables at every selected namespace
    // before descending, so raising here abandoned any overlay whose include named a whole share.
    List<String> segments = namespace == null ? List.of() : namespace.segments();
    if (segments.size() != 2) {
      return List.of();
    }
    Schema addressed = requireSchema(namespace);
    List<CatalogObjectName> tables = new ArrayList<>();
    // Through the memo the load path reads. The reconciler lists a schema and then loads every
    // table in it, so without this the schema is paged twice per pass: once here and once by the
    // first findTable.
    for (Table table : listing(addressed)) {
      tables.add(new CatalogObjectName(namespace, table.name()));
    }
    return List.copyOf(tables);
  }

  @Override
  public CatalogTable loadTable(CatalogObjectName name) {
    Schema addressed = requireSchema(name.namespace());
    Table listed = findTable(addressed, name.name());
    TableDescription described =
        translate(
            "describing " + listed.fullName(),
            () -> client.describeTable(addressed.share(), addressed.name(), name.name()));

    ExternalObjectIdentity identity = identityOf(listed);

    Map<String, String> properties = new HashMap<>(described.metadata().configuration());
    described.version().ifPresent(v -> properties.put("delta.sharing.version", Long.toString(v)));
    properties.put("delta.sharing.file-format", described.metadata().format());
    List<AccessMode> modes = statedModes(listed, described);
    properties.put(
        "delta.sharing.access-modes",
        modes.isEmpty()
            ? UNSTATED_ACCESS_MODES
            : modes.stream()
                .map(mode -> mode.name().toLowerCase(java.util.Locale.ROOT))
                .reduce((a, b) -> a + "," + b)
                .orElse(UNSTATED_ACCESS_MODES));

    // Either surface, because the protocol states auxiliary locations on the listing and on the
    // metadata action and a server may use either. Refused here rather than at the vend, because
    // this is where both are known and because a table refused here never reconciles. The protocol
    // requires a client to
    // ask for credentials once per location, root and auxiliary alike, and tells a client that
    // cannot read from several to fall back to url access or raise. The storage contract carries
    // one credential over one scope prefix, so there is nothing here to carry the rest.
    if (listed.hasAuxiliaryLocations() || described.metadata().hasAuxiliaryLocations()) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNSUPPORTED,
          "Delta Sharing table "
              + listed.fullName()
              + " reports auxiliary storage locations, which need a credential each and cannot be"
              + " published as one scoped credential");
    }

    return new CatalogTable(
        name,
        identity,
        // Always DELTA. A shared table is a Delta table; the metaData action's format.provider
        // names
        // the data file format underneath it -- parquet -- which is a different question, and
        // reporting it here is a table format the reconciler does not recognise.
        "DELTA",
        described.metadata().schemaJson(),
        described.metadata().partitionColumns(),
        Optional.empty(),
        // Present for directory access and absent otherwise, which is the protocol being explicit
        // that a url-only recipient is never told where the table lives.
        Optional.of(requireStorageLocation(addressed, name, listed, described, modes)),
        Map.copyOf(properties));
  }

  @Override
  public List<CatalogObjectName> listViews(NamespacePath namespace) {
    // Delta Sharing shares tables. There is no view concept to enumerate.
    return List.of();
  }

  @Override
  public CatalogView loadView(CatalogObjectName view) {
    throw new CatalogAccessException(
        CatalogAccessException.Code.UNSUPPORTED, "Delta Sharing does not share views");
  }

  @Override
  public Optional<VendedStorageCredentials> vendStorageCredentials(CatalogObjectName name) {
    Schema addressed = requireSchema(name.namespace());
    // The metadata endpoint, not the table listing. This is the per-read path -- a credential is
    // vended on every storage-authority resolve -- and listing the schema to find one table paged
    // the whole schema before every credential POST, which is unbounded in the schema's table
    // count and made a reconcile quadratic. The delta response format this client asks for carries
    // the access modes and auxiliary locations here, which is all the listing was consulted for.
    String qualified = addressed.share() + "." + addressed.name() + "." + name.name();
    TableDescription described =
        translate(
            "describing " + qualified,
            () -> client.describeTable(addressed.share(), addressed.name(), name.name()));
    List<AccessMode> modes = described.metadata().accessModes();
    if (modes.isEmpty() && strictAccessModes) {
      // The listing is the other place the protocol lets a server state modes, and a server may
      // use either. Strict mode refuses without asking the server, so it must not refuse a table
      // the share marked directory-accessible somewhere this path had not looked. The extra
      // listing is confined to the path an operator opted into.
      modes = findTable(addressed, name.name()).accessModes();
    }

    if (described.metadata().hasAuxiliaryLocations()) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNSUPPORTED,
          "Delta Sharing table "
              + qualified
              + " reports auxiliary storage locations, which need a credential each and cannot be"
              + " published as one scoped credential");
    }

    // A table that states its modes is believed. One that states none is ambiguous: the protocol
    // reads that as url only, and the reference server implements the credential endpoint while
    // stating nothing at all, so the default is to ask and let the server answer.
    boolean unstated = modes.isEmpty();
    if (!modes.contains(AccessMode.DIR) && (strictAccessModes || !unstated)) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNSUPPORTED,
          unstated
              ? "Delta Sharing table "
                  + qualified
                  + " states no access modes, which "
                  + STRICT_ACCESS_MODES
                  + " reads as url only"
              // The same distinction the load draws. Validation's sampler sends names straight from
              // listTables to this method without loading them, so for a table stating only modes
              // this client does not know, "offers url access only" is the message an operator
              // running a validation actually sees -- about a url mode the table never mentioned.
              : onlyUnrecognised(modes)
                  ? "Delta Sharing table "
                      + qualified
                      + " states only access modes this provider does not recognise"
                  : "Delta Sharing table "
                      + qualified
                      + " offers url access only, which cannot be vended as storage credentials");
    }

    TemporaryCredentials credentials =
        askForCredentials(addressed, name, qualified, "vending credentials for " + qualified);

    if (!credentials.hasAwsSession()) {
      // Named by cloud. A recipient on Azure or GCP is a supported configuration of the protocol
      // and an unsupported one here, and the difference between those is what an operator needs.
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNSUPPORTED,
          "Delta Sharing vended "
              + credentials.cloud()
              + " credentials for "
              + qualified
              + ", and only AWS session credentials can be published");
    }

    // Checked on the path that actually runs per read. validateStorageAccess makes the same check
    // and is only ever called from validation, so without this a server returning a credential
    // scoped somewhere unrelated or far broader than the table would go unchallenged on every
    // storage-authority resolve. The Unity provider checks it in its own vend for this reason.
    // The metadata action only. Paging the schema to look for a location was waste on exactly the
    // servers that need this path: one that omits it from its metadata omits it from its listing
    // too, and this runs on every storage-authority resolve with a fresh client, so the memo never
    // absorbs it.
    //
    // That leaves one shape unchecked, and it is a narrower claim than "loadTable already refused
    // a table without a location". loadTable resolves the listing first, so a server stating the
    // location only there reconciles, and here there is nothing to compare the scope against. The
    // check is skipped rather than failed, because the alternative is a schema listing per read --
    // the cost this path exists to avoid. validateStorageAccess does consult both surfaces, so an
    // operator sees a mis-scoped credential at validation; what is not caught is a server that
    // starts mis-scoping after that. Closing it means carrying the load's location to the vend,
    // which the vendor's fresh-client-per-resolve shape gives nowhere to put.
    // Canonical on both sides. covers() is a prefix test on strings, so encoding one side and not
    // the other makes every table whose key holds a space look scoped elsewhere.
    Optional<String> tableLocation =
        described.metadata().location().map(DeltaSharingCatalogClient::canonical);
    if (tableLocation.isPresent()) {
      // One direction only: the credential has to reach the table. A scope broader than the table
      // -- a share root, a bucket, an external-location prefix -- is left alone deliberately.
      // StorageLocations states the rule this sits under: never be stricter than the component
      // that will actually use the credential, because this path is reached only once no storage
      // authority matched, so refusing here fails a read that the catalog's own client would have
      // attempted and that S3 would have answered on the real grant.
      if (!StorageLocations.covers(canonical(credentials.location()), tableLocation.get())) {
        throw new CatalogAccessException(
            CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID,
            "Delta Sharing vended credentials that do not reach " + qualified);
      }
    }

    Map<String, String> properties = new HashMap<>(storageProperties);
    properties.put("s3.access-key-id", credentials.accessKeyId().orElseThrow());
    properties.put("s3.secret-access-key", credentials.secretAccessKey().orElseThrow());
    properties.put("s3.session-token", credentials.sessionToken().orElseThrow());

    // Servable, checked here too. requireServable ran only at the load, so on the reference-server
    // shape -- where the metaData action names no location and the coverage check above is skipped
    // -- whatever the credential endpoint returned was canonicalised and published unexamined. A
    // table reconciled from an earlier good credential could then start receiving an AWS tuple
    // scoped to a gs:// location, or one carrying userinfo, and the read failed at query time
    // instead of the provider refusing it. This needs no listing, so the shape the vend was changed
    // to avoid is unaffected.
    String scope = requireServable(credentials.location(), qualified);

    return Optional.of(
        new VendedStorageCredentials(Map.copyOf(properties), scope, credentials.expiresAt()));
  }

  @Override
  public void validateStorageAccess(
      CatalogObjectName name, VendedStorageCredentials vendedStorageCredentials) {
    Objects.requireNonNull(vendedStorageCredentials, "vendedStorageCredentials");
    Schema addressed = requireSchema(name.namespace());
    // The metadata endpoint rather than the listing, for the reason the vend uses it: one request
    // for one table instead of paging the schema to find it.
    String qualified = addressed.share() + "." + addressed.name() + "." + name.name();
    // Every surface the protocol lets a server state a location on, in the order all three paths
    // use: the metaData action, then the listing, then the scope the credential itself named. That
    // last one matters for the reference server, which states a location on neither of the first
    // two -- without it this refused every table on exactly the servers the ask-rather-than-refuse
    // default exists to support, before attempting any read.
    Optional<String> stated =
        translate(
                "describing " + qualified,
                () -> client.describeTable(addressed.share(), addressed.name(), name.name()))
            .metadata()
            .location()
            .or(() -> findTable(addressed, name.name()).location());
    String location =
        canonical(
            stated.orElseGet(
                () ->
                    Optional.of(vendedStorageCredentials.scopePrefix())
                        .filter(prefix -> !prefix.isBlank())
                        .orElseThrow(
                            () ->
                                new CatalogAccessException(
                                    CatalogAccessException.Code.UNSUPPORTED,
                                    "Delta Sharing table "
                                        + qualified
                                        + " reports no location to validate access against"))));
    // Scope first, because it answers without a network call and a credential scoped elsewhere is
    // a provider bug rather than a permission the operator can grant. Skipped where the credential
    // supplied the location, since checking a scope against itself asserts nothing; the probe below
    // still reads the store, which is what this validation is for.
    if (stated.isPresent() && !vendedStorageCredentials.covers(location)) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID,
          "Delta Sharing credentials do not cover " + qualified);
    }
    // Then the store itself. A shared table is a Delta table, so this is the same probe the Unity
    // provider runs: list one object under the Delta log and read a byte of it. Scope alone would
    // pass on a grant that cannot read, which is the failure an operator finds at query time.
    storageProbe.validate(location, vendedStorageCredentials);
  }

  @Override
  public void close() {
    client.close();
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Whether a surface stated modes and none of them are ones this client knows. */
  private static boolean onlyUnrecognised(List<AccessMode> modes) {
    return !modes.isEmpty() && modes.stream().allMatch(mode -> mode == AccessMode.OTHER);
  }

  /** Whether a surface stated its modes and directory access was not among them. */
  private static boolean refusesDir(List<AccessMode> modes) {
    return !modes.isEmpty() && !modes.contains(AccessMode.DIR);
  }

  /**
   * What the server actually stated about how this table may be read.
   *
   * <p>Two places carry it. The listing states it in either response format; the metaData action
   * states it only in the delta format, which is what this client asks for. Neither is required to,
   * so an empty answer means the server said nothing rather than that it said url.
   *
   * <p>Which surface this prefers decides nothing about whether a table may be read that way --
   * {@link #refusesDir} is asked of both surfaces separately, because the vend sees only one of
   * them. This answers the narrower question of whether anything was stated at all.
   */
  private static List<AccessMode> statedModes(Table listed, TableDescription described) {
    return listed.accessModes().isEmpty()
        ? described.metadata().accessModes()
        : listed.accessModes();
  }

  /**
   * Two components joined so that the pair can be recovered from the result.
   *
   * <p>The protocol treats a share id and a table id as opaque strings, so a bare delimiter makes
   * {@code a:b} plus {@code c} indistinguishable from {@code a} plus {@code b:c}. The reconciler
   * checks stable identities for duplicates and abandons the whole overlay on a collision, which is
   * the failure this identity exists to prevent rather than cause. Percent-encoding the delimiter
   * inside each component is enough to keep the join unambiguous.
   */
  private static String joined(String first, String second) {
    return escape(first) + ":" + escape(second);
  }

  private static String escape(String component) {
    return component.replace("%", "%25").replace(":", "%3A");
  }

  /**
   * An identity that names the share as well as the table.
   *
   * <p>The protocol scopes a table id to its share and makes {@code shareId} unique across the
   * server, so a bare table id is not unique across an Integration: two shares may carry the same
   * id, including for the same underlying table shared twice. The reconciler requires stable
   * identities to be unique across every selected table and abandons the overlay on a duplicate.
   *
   * <p>Stable only when both components are the server's own ids. Falling back to the share name,
   * or to the qualified name where the listing states no table id, keeps the identity unambiguous
   * but promises nothing about a rename surviving, so those forms are reported unstable rather than
   * claiming more than the server offered.
   */
  private static ExternalObjectIdentity identityOf(Table listed) {
    // The listing's id only. The metaData action's id identifies the underlying Delta table, which
    // the protocol calls the unique identifier for the table itself -- a different namespace from
    // the share-scoped id here. A share exposing one physical table under two names, which is a
    // legitimate configuration, reports the same value for both entries, and the reconciler
    // abandons the whole overlay on a duplicate stable identity. Falling back to it looked like it
    // recovered stability and instead risked dropping the share.
    Optional<String> tableId = listed.id();
    if (tableId.isEmpty()) {
      return new ExternalObjectIdentity(listed.fullName(), false);
    }
    return listed
        .shareId()
        .map(shareId -> new ExternalObjectIdentity(joined(shareId, tableId.get()), true))
        .orElseGet(() -> new ExternalObjectIdentity(joined(listed.share(), tableId.get()), false));
  }

  /**
   * The schema's tables, listed once for the client's lifetime.
   *
   * <p>Which is one reconcile pass: the reconciler lists a schema and then loads every table in it,
   * and each load looked the table up by paging the listing again. A table appearing mid-pass is
   * not something the pass would have seen anyway.
   */
  private List<Table> listing(Schema schema) {
    return listedTables.computeIfAbsent(
        schema,
        key ->
            translate(
                "listing tables of " + key.share() + "." + key.name(),
                () -> client.listTables(key.share(), key.name())));
  }

  /**
   * Where the table's data lives, or a refusal.
   *
   * <p>Three surfaces, because a server need not use the first two. The reference implementation
   * states a location in neither its listing nor its metadata action -- both predate directory
   * access, as its absent {@code accessModes} shows -- and names it only on the credential
   * response. Reconciling without one is not harmless: the reconciler stores no storage location
   * and leaves the Integration's own HTTPS catalog URI as the table's upstream reference, so the
   * table materializes and cannot be opened by anything that reads object storage.
   *
   * <p>So every path here ends in a location or a refusal, and nothing reconciles unreadable. A
   * table this Integration cannot read is refused at the load and counted in {@code
   * objects_skipped} rather than materialized and left to fail at query time:
   *
   * <ul>
   *   <li>url access alone, which the storage contract cannot express at all.
   *   <li>no stated modes under {@code delta.sharing.strict-access-modes}, which is the reading
   *       that knob exists to apply -- refused here without a request, since asking is exactly what
   *       it turns off.
   *   <li>no location on any surface, including the credential response.
   * </ul>
   */
  private String requireStorageLocation(
      Schema addressed,
      CatalogObjectName name,
      Table listed,
      TableDescription described,
      List<AccessMode> modes) {
    String qualified = addressed.share() + "." + addressed.name() + "." + name.name();
    // Both surfaces, not whichever one spoke first. statedModes prefers the listing, and the vend
    // reads the metaData action alone, so a server saying dir on its listing and url on its
    // metaData would otherwise load here and be refused there -- on every read, for the life of
    // the table. The load must not be more permissive than the vend, and the vend sees only the
    // metaData action, so a stated refusal on either surface is decisive. A table whose listing
    // says url and whose metaData says dir is refused too: that direction only costs a table
    // nothing could read through this path anyway, and no reading of two contradictory answers is
    // worth a vend that fails later.
    if (refusesDir(listed.accessModes()) || refusesDir(described.metadata().accessModes())) {
      // Named for what was actually stated. A table stating only modes this client does not know
      // is not a table offering url access, and saying so sent an operator looking for a url-mode
      // problem that does not exist.
      boolean unrecognised =
          onlyUnrecognised(listed.accessModes())
              || onlyUnrecognised(described.metadata().accessModes());
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNSUPPORTED,
          unrecognised
              ? "Delta Sharing table "
                  + qualified
                  + " states only access modes this provider does not recognise"
              : "Delta Sharing table "
                  + qualified
                  + " offers url access only, which cannot be vended as storage credentials");
    }
    // Only where the modes were genuinely absent. A table stating dir is not the ambiguous case
    // this knob governs, and refusing it here said "states no access modes" about a table that had
    // stated them.
    //
    // Ahead of the location, not after it. This sat below the early return, so a table with no
    // stated modes and a location on its listing was loaded and then refused by the vend, which
    // applies the same rule with no such exemption -- it reconciled and every read of it failed.
    // Knowing where a table lives says nothing about whether it may be read that way, which is the
    // only question this knob answers.
    if (modes.isEmpty() && strictAccessModes) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNSUPPORTED,
          "Delta Sharing table "
              + qualified
              + " states no access modes, which "
              + STRICT_ACCESS_MODES
              + " reads as url only");
    }
    // The metaData action first, then the listing, which is the order all three paths use. A
    // server may state a location on both surfaces and state them differently, so the paths have
    // to agree on which one wins: otherwise the table reconciles at one location, validation
    // probes another, and the vend compares the credential's scope against a third, and a passing
    // storage-access check says nothing about where a read will go. The metaData action is the
    // surface they can all reach -- the vend cannot see a listing without paging the schema on
    // every read.
    Optional<String> stated = described.metadata().location().or(listed::location);
    // Whether to ask is decided on the metaData action alone, which is the surface the vend reads.
    // statedModes prefers the listing, so a table stating dir only there took this early return
    // while the vend saw nothing stated, treated the table as ambiguous and asked -- and a server
    // that states dir on its listing and then refuses the credential endpoint reconciled here and
    // failed every read. Same regression as the unstated case, reached by the other surface.
    //
    // What both paths still share is trusting a dir the metaData action does state: neither probes
    // that, so a server contradicting its own metaData is believed. That is a policy rather than an
    // oversight -- a stated dir followed by a refusal is a server disagreeing with itself, where an
    // unstated table refusing is the ordinary shape the ask exists for -- and it costs no request
    // on the common shape. The two paths agree on it, which is what matters here.
    // Under the strict setting the vend falls back to the listing when the metaData action states
    // nothing, so the load consults it too and the two still agree. Both surfaces are already in
    // hand here, so this costs no request -- it only stops the strict setting from spending a
    // credential POST per table on a server that states its modes where the protocol defines them.
    List<AccessMode> decidedOn = strictAccessModes ? modes : described.metadata().accessModes();
    if (!decidedOn.isEmpty() && stated.isPresent()) {
      return requireServable(stated.get(), qualified);
    }
    // No blank check on what comes back. The transport refuses a credentials response that names
    // no location, as INVALID_RESPONSE, before it reaches here -- so a guard here was unreachable
    // and, by naming a per-table refusal, implied a graceful skip that does not exist. A server
    // whose credential endpoint answers 200 without a location ends the pass instead, which is the
    // rule every unreadable response follows: a response shape this client cannot read is not a
    // property of one table, and absorbing it per table leaves a broken share looking partly
    // healthy.
    // Reached two ways: dir was stated and no surface named a location, or no surface stated
    // modes at all. In the second case the credential endpoint is the only thing that can answer
    // whether directory access exists, and a stated location does not answer it -- the protocol
    // lets a url-only server name one. So a server that omits its access modes, names a location
    // and refuses this endpoint has to be asked, not assumed: asking is what the default setting
    // is. askForCredentials classifies the answer, so a refusal arrives as UNSUPPORTED and skips
    // this table rather than reconciling one whose every read fails.
    TemporaryCredentials answered =
        askForCredentials(addressed, name, qualified, "asking where " + qualified + " lives");
    String vended = answered.location();
    // The location the credential names is the table's root, whatever shape its path has. The
    // protocol answers a request naming no location with the table's main location, and a Delta
    // table may sit directly at a bucket root -- deltaLogPrefix("") answers _delta_log/ for that
    // case on purpose.
    //
    // Whether a credential is scoped where the table actually lives is settled by reading the
    // Delta log under it, which validateStorageAccess does. The shape of a path cannot settle it,
    // and a check here that guessed from the shape would refuse tables validation reads happily.
    // A location a surface stated outranks the credential's scope, which only stands in for one --
    // but it has to be reachable with the credential the server just answered with. Where both are
    // known and disjoint, reconciling the stated one materialises a table every read of which is
    // scoped elsewhere, and validation is optional so nothing else need catch it. The vend makes
    // this comparison for a location the metaData action states; making it here covers the surface
    // the vend cannot see.
    if (stated.isPresent()
        && !StorageLocations.covers(canonical(vended), canonical(stated.get()))) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID,
          "Delta Sharing answered for " + qualified + " with credentials that do not reach it");
    }
    return requireServable(stated.orElse(vended), qualified);
  }

  /**
   * A location this provider can actually read, or a refusal.
   *
   * <p>The storage probe addresses S3 and the vend publishes an AWS session, so an {@code abfss://}
   * or {@code gs://} table would load, reconcile, and then fail at every scan when the vend refused
   * its cloud -- the unreadable materialized table the other refusals here exist to prevent,
   * reached by a different route and not counted in {@code objects_skipped} either.
   */
  private static String requireServable(String location, String qualified) {
    if (!DeltaLogStorageProbe.s3Serves(location)) {
      // The scheme, never the location. This message reaches operator logs through the throwable
      // the reconciler records, and the location is the server's: a query or fragment on one is a
      // signature or a SAS token, userinfo is a password, and a control character in it forges a
      // log line. The scheme is the whole answer to which cloud, and cannot carry any of those.
      String scheme = schemeOf(location);
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNSUPPORTED,
          "s3".equals(scheme)
              // Same answer from s3Serves, two different problems. Reporting the cloud for an s3
              // location whose bucket would not parse told an operator the opposite of what was
              // wrong, and sent them to look at a provider that reads S3 perfectly well.
              ? "Delta Sharing table "
                  + qualified
                  + " names an s3 location this provider could not read a bucket from"
              : "Delta Sharing table "
                  + qualified
                  + " lives on "
                  + scheme
                  + ", which this provider cannot read: it vends AWS session credentials for S3");
    }
    // The canonical form, not the one the server sent. This is the value that reaches
    // CatalogTable.storageLocation and then UpstreamRef, so it has to be one the read path can
    // parse; see canonical().
    return canonical(location);
  }

  /**
   * The form of a location this provider publishes, vends and compares.
   *
   * <p>The rule lives in {@link DeltaLogStorageProbe#canonicalLocation}, beside the probe whose
   * space encoding created the need for it, because the Unity provider needs the same answer and
   * had received the probe half of this without the publish half.
   */
  private static String canonical(String location) {
    return DeltaLogStorageProbe.canonicalLocation(location);
  }

  /** A location's scheme alone, which names the cloud and can hold nothing else. */
  private static String schemeOf(String location) {
    try {
      // Spaces encoded first. An S3 object key may hold one and java.net.URI may not, so a legal
      // location threw here and was reported as having no readable scheme at all. Other characters
      // URI rejects and S3 permits are still refused; a space is the one that occurs.
      String scheme = java.net.URI.create(canonical(location)).getScheme();
      return scheme == null ? "no addressable scheme" : scheme.toLowerCase(java.util.Locale.ROOT);
    } catch (IllegalArgumentException notAUri) {
      return "no addressable scheme";
    }
  }

  /**
   * The credential call, classified as the capability boundary it answers.
   *
   * <p>A server without the endpoint answers NOT_FOUND, and the refusal the protocol defines for a
   * table not offering directory access is INVALID_REQUEST. Both mean the same thing to a caller --
   * directory access is not available for this table -- so both are reported as unsupported rather
   * than as a missing table or a bad request.
   *
   * <p>Not gated on whether the table stated its modes, so that one HTTP 400 means the same thing
   * on the load path and the vend path. The server's answer settles it either way: a table whose
   * listing claims dir and whose credential endpoint refuses does not have directory access,
   * whatever the listing said.
   */
  private TemporaryCredentials askForCredentials(
      Schema addressed, CatalogObjectName name, String qualified, String action) {
    try {
      return client.temporaryTableCredentials(
          addressed.share(), addressed.name(), name.name(), null);
    } catch (DeltaSharingException failure) {
      // A 3xx is excluded. The transport folds a refused redirect into INVALID_REQUEST, and its
      // own comment says what that means: the base URI names something other than a sharing
      // server. Read here as the protocol's per-table refusal, an auth proxy or a trailing-slash
      // redirect in front of the server reported every table as offering no directory access --
      // a configuration error arriving as a per-table capability limit, table by table, with the
      // integration looking partly healthy rather than misconfigured.
      // 400 exactly, not every INVALID_REQUEST. The transport folds 400, 405 and 422 into that
      // one failure, and only 400 is the protocol's own statement that a table does not offer
      // directory access. A 405 is a GET-only proxy or a gateway rule and a 422 is neither -- both
      // are configuration in front of the server, and reading them as a capability limit reported
      // every table as lacking dir access while the real fault went unnamed. They stay a per-table
      // skip either way, since INVALID_CONFIGURATION also describes one branch; what changes is
      // the issue an operator is shown and the message that comes with it.
      boolean protocolRefusal =
          failure.failure() == DeltaSharingException.Failure.NOT_FOUND
              || (failure.failure() == DeltaSharingException.Failure.INVALID_REQUEST
                  && failure.statusCode() == 400);
      if (protocolRefusal) {
        throw new CatalogAccessException(
            CatalogAccessException.Code.UNSUPPORTED,
            "Delta Sharing did not offer directory access for " + qualified,
            failure);
      }
      throw new CatalogAccessException(
          codeFor(failure.failure()), messageFor(action, failure), failure);
    }
  }

  private Schema requireSchema(NamespacePath namespace) {
    List<String> segments = namespace == null ? List.of() : namespace.segments();
    if (segments.size() != 2) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Delta Sharing addresses a table as share.schema.table, so its namespace has two"
              + " segments, not "
              + segments.size());
    }
    return new Schema(segments.get(0), segments.get(1));
  }

  /**
   * The listed table, which carries the share id that describing it does not.
   *
   * <p>Only the load needs this. The vend and the storage probe read one table's metadata instead,
   * because paging a whole schema to find one table is not something a per-read path should do.
   */
  private Table findTable(Schema schema, String name) {
    // Memoised for the client's lifetime, which is one reconcile pass: the reconciler lists a
    // schema once and then loads every table in it, and each load looked the table up by paging
    // the whole listing again. Only loadTable reaches here -- the vend and the storage probe read
    // one table's metadata instead -- so the window is a single traversal of a schema it has just
    // listed, and a table appearing mid-pass is not something the pass would have seen anyway.
    for (Table table : listing(schema)) {
      if (table.name().equals(name)) {
        return table;
      }
    }
    throw new CatalogAccessException(
        CatalogAccessException.Code.NOT_FOUND,
        "Delta Sharing share " + schema.share() + "." + schema.name() + " does not share " + name);
  }

  private <T> T translate(String action, java.util.function.Supplier<T> call) {
    try {
      return call.get();
    } catch (DeltaSharingException e) {
      throw new CatalogAccessException(codeFor(e.failure()), messageFor(action, e), e);
    }
  }

  private void translate(String action, Runnable call) {
    translate(
        action,
        () -> {
          call.run();
          return null;
        });
  }

  private static String messageFor(String action, DeltaSharingException e) {
    return "Delta Sharing failed while " + action + ": " + e.getMessage();
  }

  /**
   * The recipient's classification, mapped to the SPI's.
   *
   * <p>The distinction that matters is terminal against retryable. A rejected token and a share not
   * granted will answer the same way on every attempt; a rate limit, a server error and a transport
   * failure will not.
   */
  private static CatalogAccessException.Code codeFor(DeltaSharingException.Failure failure) {
    return switch (failure) {
      case UNAUTHENTICATED -> CatalogAccessException.Code.UNAUTHENTICATED;
      case PERMISSION_DENIED -> CatalogAccessException.Code.PERMISSION_DENIED;
      case NOT_FOUND -> CatalogAccessException.Code.NOT_FOUND;
      case RATE_LIMITED, SERVER_ERROR, TRANSPORT, TRANSIENT ->
          CatalogAccessException.Code.UNAVAILABLE;
      case INTERRUPTED -> CatalogAccessException.Code.TIMEOUT;
      // Split, because they describe different scopes. INVALID_REQUEST is the protocol's own
      // per-table refusal, and INVALID_CONFIGURATION is per-branch, so the walk steps over it.
      // INVALID_RESPONSE is a body this client cannot read -- a proxy error page, a version
      // mismatch reshaping every table -- which describes the catalog. INTERNAL is outside
      // describesOneBranch, so a reconcile fails on it rather than recording every table as
      // unobserved and leaving a broken share looking partially healthy. Unity splits them the
      // same way and for the same reason.
      case INVALID_REQUEST -> CatalogAccessException.Code.INVALID_CONFIGURATION;
      case INVALID_RESPONSE -> CatalogAccessException.Code.INTERNAL;
      // Not UNSUPPORTED. That code now means the provider will never do this: the service skips
      // the table and reports CIVI_..._UNSUPPORTED, and the vendor reports a deterministic refusal.
      // OTHER is the catch-all for a status this client does not classify -- 402, 407, 409, 423,
      // 451 -- which is a real upstream or configuration problem, and naming it unsupported tells
      // an operator there is nothing to fix. UNAVAILABLE keeps the status in the message and
      // treats an unknown condition as one a later attempt might not meet.
      case OTHER -> CatalogAccessException.Code.UNAVAILABLE;
    };
  }
}

package ai.floedb.metacat.service.repo.util;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Centralized construction of pointer keys, blob URIs,
 * and index keys for name/index lookups.
 *
 * Conventions:
 * - All pointer keys are absolute paths under /tenants/{tid}/... and use URL-encoded IDs.
 * - Tenant IDs are normalized to lowercase in keys/URIs.
 * - Blob URIs are in the `mem://` scheme but may be backed by any object store implementation.
 * - Index keys live under /_index/... and store small protobufs (e.g., NameRef, ResourceId).
 *
 * Pointers vs. Indexes:
 * - Pointers are the OCC gate (versioned). Blob writes + pointer CAS define atomic mutations.
 * - Indexes are derived and can be repaired by background scrubbers.
 */
public final class Keys {
  private Keys() {}

  /** URL-encode a key segment using UTF-8. */
  private static String enc(String s)
  {
    return URLEncoder.encode(s, StandardCharsets.UTF_8); 
  }

  /** Normalize tenant ID for keys. */
  private static String normTenant(String tid) {
    return tid.toLowerCase();
  }

  // -------------------
  // Catalog
  // -------------------

  /** Canonical pointer for a catalog. */
  public static String catPtr(String tid, String catId) {
    return "/tenants/" + normTenant(tid) + "/catalogs/by-id/" + enc(catId);
  }

  /** Blob URI for a catalog protobuf. */
  public static String catBlob(String tid, String catId) {
    return "mem://tenants/" + normTenant(tid) + "/catalogs/" + enc(catId) + "/catalog.pb";
  }

  // -------------------
  // Namespace (under a catalog)
  // -------------------

  /**
   * Canonical pointer for a namespace under its owning catalog.
   */
  public static String nsPtr(String tid, String catId, String nsId) {
    return "/tenants/" + normTenant(tid) + "/catalogs/" + enc(catId) + "/namespaces/by-id/" + enc(nsId);
  }

  /** Blob URI for a namespace protobuf. */
  public static String nsBlob(String tid, String catId, String nsId) {
    return "mem://tenants/" + normTenant(tid) + "/catalogs/" + enc(catId) + "/namespaces/" + enc(nsId) + "/namespace.pb";
  }

  // -------------------
  // Table
  // -------------------

  /**
   * Canonical pointer for a table.
   * This pointer is not nested by catalog/namespace and is the OCC gate for table mutations.
   */
  public static String tblCanonicalPtr(String tid, String tblId) {
    return "/tenants/" + normTenant(tid) + "/tables/" + enc(tblId);
  }

  /**
   * Namespace-scoped pointer for a table (secondary lookup path).
   * Points to the SAME blob as the canonical pointer, but lives under the owning catalog/namespace.
   */
  public static String tblPtr(String tid, String catId, String nsId, String tblId) {
    return "/tenants/" + normTenant(tid) + "/catalogs/" + enc(catId) + "/namespaces/" + enc(nsId) + "/tables/by-id/" + enc(tblId);
  }

  /** Blob URI for a table protobuf. */
  public static String tblBlob(String tid, String tblId) {
    return "mem://tenants/" + normTenant(tid) + "/tables/" + enc(tblId) + "/table.pb";
  }

  // -------------------
  // Snapshots (under table)
  // -------------------

  /** Pointer to a specific snapshot for a table. */
  public static String snapPtr(String tid, String tblId, long snapshotId) {
    return "/tenants/" + normTenant(tid) + "/tables/" + enc(tblId) + "/snapshots/" + snapshotId;
  }

  /** Pointer prefix to list snapshots for a table. */
  public static String snapPtr(String tid, String tblId) {
    return "/tenants/" + normTenant(tid) + "/tables/" + enc(tblId) + "/snapshots/";
  }

  /** Blob URI for a snapshot protobuf. */
  public static String snapBlob(String tid, String tblId, long snapshotId) {
    return "mem://tenants/" + normTenant(tid) + "/tables/" + enc(tblId) + "/snapshots/" + snapshotId + "/snapshot.pb";
  }

  // -------------------
  // Indexes: catalogs
  // -------------------

  /** Index: catalog by display name to NameRef. */
  public static String idxCatByName(String tid, String name) {
    return "/tenants/" + normTenant(tid) + "/_index/catalogs/by-name/" + enc(name);
  }

  /** List all catalogs by name prefix */
  public static String idxCatByNamePrefix(String tid) {
    return "/tenants/" + normTenant(tid) + "/_index/catalogs/by-name/";
  }

  /** Index: catalog by id to NameRef. */
  public static String idxCatById(String tid, String id) {
    return "/tenants/" + normTenant(tid) + "/_index/catalogs/by-id/" + enc(id);
  }

  // -------------------
  // Indexes: namespaces
  // -------------------

  /** Index: namespace by full path (catalog_id + path segments) to NameRef. */
  public static String idxNsByPath(String tid, String catalogId, String path) {
    String[] parts = path.split("/");
    String joined = String.join("/", Arrays.stream(parts).map(Keys::enc).toArray(String[]::new));
    return "/tenants/" + normTenant(tid) + "/_index/namespaces/by-path/" + enc(catalogId) + "/" + joined;
  }

  /** List all namespaces under a catalog */
  public static String idxNsByPathPrefix(String tid, String catalogId) {
    return "/tenants/" + normTenant(tid) + "/_index/namespaces/by-path/" + enc(catalogId) + "/";
  }

  /** Index: namespace by id to NameRef. */
  public static String idxNsById(String tid, String id) {
    return "/tenants/" + normTenant(tid) + "/_index/namespaces/by-id/" + enc(id);
  }

  /** Index: namespace owner (catalog id) by namespace id to catalog ResourceId. */
  public static String idxNsOwnerById(String tid, String nsId) {
    return "/tenants/" + normTenant(tid) + "/_index/namespaces/owner/by-id/" + enc(nsId);
  }

  /** Restrict to a subpath under the catalog (e.g., ["db","schema"]) */
  public static String idxNsByPathPrefix(String tid, String catalogId, List<String> path) {
    String joined = String.join("/", path.stream().map(Keys::enc).toArray(String[]::new));
    return "/tenants/" + normTenant(tid) + "/_index/namespaces/by-path/" + enc(catalogId)
        + (joined.isEmpty() ? "/" : "/" + joined + "/");
  }

  // -------------------
  // Indexes: tables
  // -------------------

  /** Index: table by namespace + table_id to NameRef. */
  public static String idxTblByNamespace(String tid, String nsId, String tableId) {
    return "/tenants/" + normTenant(tid) + "/_index/tables/by-namespace/" + enc(nsId) + "/" + enc(tableId);
  }

  /** Index: table by catalog_id + ns path + leaf to NameRef (full name). */
  public static String idxTblByName(String tid, String catalogId, List<String> nsPath, String leaf) {
    String joined = String.join("/", nsPath.stream().map(Keys::enc).toArray(String[]::new));
    return "/tenants/" + normTenant(tid) + "/_index/tables/by-name/" + enc(catalogId)
        + (joined.isEmpty() ? "" : "/" + joined) + "/" + enc(leaf);
  }

  /** List prefix: all tables under a namespace (by leaf). */
  public static String idxTblByNamespaceLeafPrefix(String tid, String nsId) {
    return "/tenants/" + normTenant(tid) + "/_index/tables/by-namespace/" + enc(nsId) + "/";
  }

  /** List prefix: all tables under a catalog_id + ns path. */
  public static String idxTblByNamePrefix(String tid, String catalogId, List<String> nsPath) {
    String joined = String.join("/", nsPath.stream().map(Keys::enc).toArray(String[]::new));
    return "/tenants/" + normTenant(tid) + "/_index/tables/by-name/" + enc(catalogId)
        + (joined.isEmpty() ? "/" : "/" + joined + "/");
  }

  /** Index: table by namespace + leaf (no tableId in key) to NameRef (full name). */
  public static String idxTblByNamespaceLeaf(String tid, String nsId, String leaf) {
    return "/tenants/" + normTenant(tid) + "/_index/tables/by-namespace/" + enc(nsId) + "/" + enc(leaf);
  }

  /** Index: table by id to NameRef (full name). */
  public static String idxTblById(String tid, String id) {
    return "/tenants/" + normTenant(tid) + "/_index/tables/by-id/" + enc(id);
  }

  // -------------------
  // Idempotency log
  // -------------------

  /** Idempotency record key: per-tenant, per-op, per-user-supplied idempotency key. */
  public static String idemKey(String tid, String op, String key) {
    return "/tenants/" + normTenant(tid) + "/idempotency/" + enc(op) + "/" + enc(key);
  }

  // -------------------
  // Helpers
  // -------------------

  /**
   * Convenience to derive a blob URI for a given pointer key. Primarily used by in-memory stores.
   * Example: mem:///tenants/tid/_index/tables/by-id/abc/entry.pb
   */
  public static String memUriFor(String pointerKey, String leaf) {
    String base = pointerKey.startsWith("/") ? pointerKey.substring(1) : pointerKey;
    return "mem://" + base + (leaf.startsWith("/") ? leaf : "/" + leaf);
  }
}

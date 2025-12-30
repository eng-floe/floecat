package ai.floedb.floecat.service.gc;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
public class CasBlobGc {

  @Inject BlobStore blobStore;
  @Inject PointerStore pointerStore;

  public record Result(
      int pointersScanned, int blobsScanned, int blobsDeleted, int referenced, int tablesScanned) {}

  public Result runForAccount(String accountId) {
    final var cfg = ConfigProvider.getConfig();
    final int pageSize =
        cfg.getOptionalValue("floecat.gc.cas.page-size", Integer.class).orElse(500);

    Set<String> referenced = new HashSet<>();
    List<String> tableIds = new ArrayList<>();
    int pointersScanned = 0;

    var accountPtr = pointerStore.get(Keys.accountPointerById(accountId)).orElse(null);
    if (accountPtr != null && !accountPtr.getBlobUri().isBlank()) {
      referenced.add(normalizeKey(accountPtr.getBlobUri()));
      pointersScanned++;
    }

    pointersScanned +=
        collectPointers(accountPrefix(accountId, "/catalogs/by-id/"), referenced, null, pageSize);
    pointersScanned +=
        collectPointers(accountPrefix(accountId, "/namespaces/by-id/"), referenced, null, pageSize);
    pointersScanned +=
        collectPointers(accountPrefix(accountId, "/tables/by-id/"), referenced, tableIds, pageSize);
    pointersScanned +=
        collectPointers(accountPrefix(accountId, "/views/by-id/"), referenced, null, pageSize);
    pointersScanned +=
        collectPointers(accountPrefix(accountId, "/connectors/by-id/"), referenced, null, pageSize);

    int tablesScanned = 0;
    for (String tableId : tableIds) {
      tablesScanned++;
      String prefix =
          "/accounts/" + encode(accountId) + "/tables/" + encode(tableId) + "/snapshots/by-id/";
      pointersScanned += collectPointers(prefix, referenced, null, pageSize);
    }

    int blobsScanned = 0;
    int blobsDeleted = 0;

    var account =
        deleteUnreferenced(
            accountPrefix(accountId, "/account/"),
            referenced,
            key -> key.contains("/account/"),
            pageSize);
    blobsScanned += account.scanned();
    blobsDeleted += account.deleted();

    var catalogs =
        deleteUnreferenced(
            accountPrefix(accountId, "/catalogs/"),
            referenced,
            key -> key.contains("/catalog/"),
            pageSize);
    blobsScanned += catalogs.scanned();
    blobsDeleted += catalogs.deleted();

    var namespaces =
        deleteUnreferenced(
            accountPrefix(accountId, "/namespaces/"),
            referenced,
            key -> key.contains("/namespace/"),
            pageSize);
    blobsScanned += namespaces.scanned();
    blobsDeleted += namespaces.deleted();

    var tables =
        deleteUnreferenced(
            accountPrefix(accountId, "/tables/"),
            referenced,
            key -> key.contains("/table/"),
            pageSize);
    blobsScanned += tables.scanned();
    blobsDeleted += tables.deleted();

    var snapshots =
        deleteUnreferenced(
            accountPrefix(accountId, "/tables/"),
            referenced,
            key -> key.contains("/snapshots/") && key.contains("/snapshot/"),
            pageSize);
    blobsScanned += snapshots.scanned();
    blobsDeleted += snapshots.deleted();

    var views =
        deleteUnreferenced(
            accountPrefix(accountId, "/views/"),
            referenced,
            key -> key.contains("/view/"),
            pageSize);
    blobsScanned += views.scanned();
    blobsDeleted += views.deleted();

    var connectors =
        deleteUnreferenced(
            accountPrefix(accountId, "/connectors/"),
            referenced,
            key -> key.contains("/connector/"),
            pageSize);
    blobsScanned += connectors.scanned();
    blobsDeleted += connectors.deleted();

    return new Result(
        pointersScanned, blobsScanned, blobsDeleted, referenced.size(), tablesScanned);
  }

  private int collectPointers(
      String prefix, Set<String> referenced, List<String> tableIds, int pageSize) {
    String token = "";
    int scanned = 0;

    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, pageSize, token, next);
      for (Pointer p : pointers) {
        scanned++;
        if (p.getBlobUri() != null && !p.getBlobUri().isBlank()) {
          referenced.add(normalizeKey(p.getBlobUri()));
        }
        if (tableIds != null) {
          String id = decodeSuffix(prefix, p.getKey());
          if (id != null && !id.isBlank()) {
            tableIds.add(id);
          }
        }
      }

      token = next.toString();
      if (token.isEmpty()) {
        break;
      }
    }
    return scanned;
  }

  private record DeleteResult(int scanned, int deleted) {}

  private DeleteResult deleteUnreferenced(
      String prefix, Set<String> referenced, Predicate<String> isCandidate, int pageSize) {
    String token = "";
    int scanned = 0;
    int deleted = 0;

    while (true) {
      BlobStore.Page page = blobStore.list(prefix, pageSize, token);
      for (String key : page.keys()) {
        scanned++;
        String normalized = normalizeKey(key);
        if (!isCandidate.test(normalized)) {
          continue;
        }
        if (!referenced.contains(normalized)) {
          if (blobStore.delete(key)) {
            deleted++;
          }
        }
      }
      token = page.nextToken();
      if (token == null || token.isBlank()) {
        break;
      }
    }

    return new DeleteResult(scanned, deleted);
  }

  private static String accountPrefix(String accountId, String suffix) {
    return "/accounts/" + encode(accountId) + suffix;
  }

  private static String decodeSuffix(String prefix, String fullKey) {
    if (fullKey == null || !fullKey.startsWith(prefix)) {
      return null;
    }
    String suffix = fullKey.substring(prefix.length());
    if (suffix.isBlank()) {
      return null;
    }
    return URLDecoder.decode(suffix, StandardCharsets.UTF_8);
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static String normalizeKey(String key) {
    if (key == null) {
      return "";
    }
    return key.startsWith("/") ? key.substring(1) : key;
  }
}

package ai.floedb.floecat.systemcatalog.registry;

import ai.floedb.floecat.systemcatalog.def.*;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Immutable view of all builtin SQL objects for a specific engine kind. */
public final class SystemEngineCatalog {

  private final String engineKind;
  private final String fingerprint;

  private final List<SystemFunctionDef> functions;
  private final Map<String, List<SystemFunctionDef>> functionsByName;

  private final List<SystemOperatorDef> operators;
  private final Map<String, SystemOperatorDef> operatorsByName;

  private final List<SystemTypeDef> types;
  private final Map<String, SystemTypeDef> typesByName;

  private final List<SystemCastDef> casts;
  private final Map<String, SystemCastDef> castsByName;

  private final List<SystemCollationDef> collations;
  private final Map<String, SystemCollationDef> collationsByName;

  private final List<SystemAggregateDef> aggregates;
  private final Map<String, SystemAggregateDef> aggregatesByName;

  private final List<SystemNamespaceDef> namespaces;
  private final Map<String, SystemNamespaceDef> namespacesByName;

  private final List<SystemTableDef> tables;
  private final Map<String, SystemTableDef> tablesByName;

  private final List<SystemViewDef> views;
  private final Map<String, SystemViewDef> viewsByName;

  private SystemEngineCatalog(
      String engineKind,
      String fingerprint,
      List<SystemFunctionDef> functions,
      Map<String, List<SystemFunctionDef>> functionsByName,
      List<SystemOperatorDef> operators,
      Map<String, SystemOperatorDef> operatorsByName,
      List<SystemTypeDef> types,
      Map<String, SystemTypeDef> typesByName,
      List<SystemCastDef> casts,
      Map<String, SystemCastDef> castsByName,
      List<SystemCollationDef> collations,
      Map<String, SystemCollationDef> collationsByName,
      List<SystemAggregateDef> aggregates,
      Map<String, SystemAggregateDef> aggregatesByName,
      List<SystemNamespaceDef> namespaces,
      Map<String, SystemNamespaceDef> namespacesByName,
      List<SystemTableDef> tables,
      Map<String, SystemTableDef> tablesByName,
      List<SystemViewDef> views,
      Map<String, SystemViewDef> viewsByName) {

    this.engineKind = engineKind;
    this.fingerprint = fingerprint;
    this.functions = functions;
    this.functionsByName = functionsByName;
    this.operators = operators;
    this.operatorsByName = operatorsByName;
    this.types = types;
    this.typesByName = typesByName;
    this.casts = casts;
    this.castsByName = castsByName;
    this.collations = collations;
    this.collationsByName = collationsByName;
    this.aggregates = aggregates;
    this.aggregatesByName = aggregatesByName;
    this.namespaces = namespaces;
    this.namespacesByName = namespacesByName;
    this.tables = tables;
    this.tablesByName = tablesByName;
    this.views = views;
    this.viewsByName = viewsByName;
  }

  /** Builds a materialised catalog snapshot from parsed builtin data. */
  public static SystemEngineCatalog from(String engineKind, SystemCatalogData data) {

    List<SystemFunctionDef> functions = copy(data.functions());
    List<SystemOperatorDef> operators = copy(data.operators());
    List<SystemTypeDef> types = copy(data.types());
    List<SystemCastDef> casts = copy(data.casts());
    List<SystemCollationDef> collations = copy(data.collations());
    List<SystemAggregateDef> aggregates = copy(data.aggregates());

    List<SystemNamespaceDef> namespaces = copy(data.namespaces());
    List<SystemTableDef> tables = copy(data.tables());
    List<SystemViewDef> views = copy(data.views());
    String fingerprint = computeFingerprint(engineKind, data);

    return new SystemEngineCatalog(
        engineKind,
        fingerprint,
        functions,
        indexMulti(functions, f -> NameRefUtil.canonical(f.name())),
        operators,
        indexUnique(operators, o -> NameRefUtil.canonical(o.name())),
        types,
        indexUnique(types, t -> NameRefUtil.canonical(t.name())),
        casts,
        indexUnique(casts, c -> NameRefUtil.canonical(c.name())),
        collations,
        indexUnique(collations, c -> NameRefUtil.canonical(c.name())),
        aggregates,
        indexUnique(aggregates, a -> NameRefUtil.canonical(a.name())),
        namespaces,
        indexUnique(namespaces, n -> NameRefUtil.canonical(n.name())),
        tables,
        indexUnique(tables, t -> NameRefUtil.canonical(t.name())),
        views,
        indexUnique(views, v -> NameRefUtil.canonical(v.name())));
  }

  public String engineKind() {
    return engineKind;
  }

  public String fingerprint() {
    return fingerprint;
  }

  public List<SystemFunctionDef> functions() {
    return functions;
  }

  public List<SystemFunctionDef> functions(String name) {
    return functionsByName.getOrDefault(name, List.of());
  }

  public List<SystemOperatorDef> operators() {
    return operators;
  }

  public Optional<SystemOperatorDef> operator(String name) {
    return Optional.ofNullable(operatorsByName.get(name));
  }

  public List<SystemTypeDef> types() {
    return types;
  }

  public Optional<SystemTypeDef> type(String name) {
    return Optional.ofNullable(typesByName.get(name));
  }

  public List<SystemCastDef> casts() {
    return casts;
  }

  public Optional<SystemCastDef> cast(String name) {
    return Optional.ofNullable(castsByName.get(name));
  }

  public List<SystemCollationDef> collations() {
    return collations;
  }

  public Optional<SystemCollationDef> collation(String name) {
    return Optional.ofNullable(collationsByName.get(name));
  }

  public List<SystemAggregateDef> aggregates() {
    return aggregates;
  }

  public Optional<SystemAggregateDef> aggregate(String name) {
    return Optional.ofNullable(aggregatesByName.get(name));
  }

  public List<SystemNamespaceDef> namespaces() {
    return namespaces;
  }

  public Optional<SystemNamespaceDef> namespace(String name) {
    return Optional.ofNullable(namespacesByName.get(name));
  }

  public List<SystemTableDef> tables() {
    return tables;
  }

  public Optional<SystemTableDef> table(String name) {
    return Optional.ofNullable(tablesByName.get(name));
  }

  public List<SystemViewDef> views() {
    return views;
  }

  public Optional<SystemViewDef> view(String name) {
    return Optional.ofNullable(viewsByName.get(name));
  }

  private static <T> List<T> copy(List<T> values) {
    return List.copyOf(values == null ? List.of() : values);
  }

  private static <T> Map<String, T> indexUnique(List<T> defs, Function<T, String> keyFn) {
    return defs.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                keyFn, Function.identity(), (existing, replacement) -> replacement));
  }

  private static <T> Map<String, List<T>> indexMulti(List<T> defs, Function<T, String> keyFn) {
    return defs.stream()
        .collect(
            Collectors.collectingAndThen(
                Collectors.groupingBy(
                    keyFn, Collectors.collectingAndThen(Collectors.toList(), List::copyOf)),
                Map::copyOf));
  }

  private static String computeFingerprint(String engineKind, SystemCatalogData data) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");

      // Salt with engine kind to avoid cross-engine collisions
      digest.update(engineKind.getBytes(java.nio.charset.StandardCharsets.UTF_8));

      // Use canonical protobuf serialization for deterministic hashing
      var proto = SystemCatalogProtoMapper.toProto(data);
      var dos =
          new java.security.DigestOutputStream(java.io.OutputStream.nullOutputStream(), digest);
      proto.writeTo(dos);
      dos.flush();
      return HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException | java.io.IOException e) {
      throw new IllegalStateException("Failed to compute fingerprint", e);
    }
  }
}

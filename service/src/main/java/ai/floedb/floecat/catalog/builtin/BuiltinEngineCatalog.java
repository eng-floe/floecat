package ai.floedb.floecat.catalog.builtin;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Immutable view of all builtin SQL objects for a specific engine kind. */
public final class BuiltinEngineCatalog {

  private final String engineKind;
  private final String fingerprint;

  private final List<BuiltinFunctionDef> functions;
  private final Map<String, List<BuiltinFunctionDef>> functionsByName;

  private final List<BuiltinOperatorDef> operators;
  private final Map<String, BuiltinOperatorDef> operatorsByName;

  private final List<BuiltinTypeDef> types;
  private final Map<String, BuiltinTypeDef> typesByName;

  private final List<BuiltinCastDef> casts;
  private final Map<String, BuiltinCastDef> castsByName;

  private final List<BuiltinCollationDef> collations;
  private final Map<String, BuiltinCollationDef> collationsByName;

  private final List<BuiltinAggregateDef> aggregates;
  private final Map<String, BuiltinAggregateDef> aggregatesByName;

  private BuiltinEngineCatalog(
      String engineKind,
      String fingerprint,
      List<BuiltinFunctionDef> functions,
      Map<String, List<BuiltinFunctionDef>> functionsByName,
      List<BuiltinOperatorDef> operators,
      Map<String, BuiltinOperatorDef> operatorsByName,
      List<BuiltinTypeDef> types,
      Map<String, BuiltinTypeDef> typesByName,
      List<BuiltinCastDef> casts,
      Map<String, BuiltinCastDef> castsByName,
      List<BuiltinCollationDef> collations,
      Map<String, BuiltinCollationDef> collationsByName,
      List<BuiltinAggregateDef> aggregates,
      Map<String, BuiltinAggregateDef> aggregatesByName) {

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
  }

  /** Builds a materialised catalog snapshot from parsed builtin data. */
  public static BuiltinEngineCatalog from(String engineKind, BuiltinCatalogData data) {

    List<BuiltinFunctionDef> functions = copy(data.functions());
    List<BuiltinOperatorDef> operators = copy(data.operators());
    List<BuiltinTypeDef> types = copy(data.types());
    List<BuiltinCastDef> casts = copy(data.casts());
    List<BuiltinCollationDef> collations = copy(data.collations());
    List<BuiltinAggregateDef> aggregates = copy(data.aggregates());

    String fingerprint = computeFingerprint(engineKind, data);

    return new BuiltinEngineCatalog(
        engineKind,
        fingerprint,
        functions,
        indexMulti(functions, f -> BuiltinNameUtil.canonical(f.name())),
        operators,
        indexUnique(operators, o -> BuiltinNameUtil.canonical(o.name())),
        types,
        indexUnique(types, t -> BuiltinNameUtil.canonical(t.name())),
        casts,
        indexUnique(casts, c -> BuiltinNameUtil.canonical(c.name())),
        collations,
        indexUnique(collations, c -> BuiltinNameUtil.canonical(c.name())),
        aggregates,
        indexUnique(aggregates, a -> BuiltinNameUtil.canonical(a.name())));
  }

  public String engineKind() {
    return engineKind;
  }

  public String fingerprint() {
    return fingerprint;
  }

  public List<BuiltinFunctionDef> functions() {
    return functions;
  }

  public List<BuiltinFunctionDef> functions(String name) {
    return functionsByName.getOrDefault(name, List.of());
  }

  public List<BuiltinOperatorDef> operators() {
    return operators;
  }

  public Optional<BuiltinOperatorDef> operator(String name) {
    return Optional.ofNullable(operatorsByName.get(name));
  }

  public List<BuiltinTypeDef> types() {
    return types;
  }

  public Optional<BuiltinTypeDef> type(String name) {
    return Optional.ofNullable(typesByName.get(name));
  }

  public List<BuiltinCastDef> casts() {
    return casts;
  }

  public Optional<BuiltinCastDef> cast(String name) {
    return Optional.ofNullable(castsByName.get(name));
  }

  public List<BuiltinCollationDef> collations() {
    return collations;
  }

  public Optional<BuiltinCollationDef> collation(String name) {
    return Optional.ofNullable(collationsByName.get(name));
  }

  public List<BuiltinAggregateDef> aggregates() {
    return aggregates;
  }

  public Optional<BuiltinAggregateDef> aggregate(String name) {
    return Optional.ofNullable(aggregatesByName.get(name));
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

  private static String computeFingerprint(String engineKind, BuiltinCatalogData data) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");

      // Salt with engine kind to avoid cross-engine collisions
      digest.update(engineKind.getBytes(java.nio.charset.StandardCharsets.UTF_8));

      // Use canonical protobuf serialization for deterministic hashing
      var proto = BuiltinCatalogProtoMapper.toProto(data);
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

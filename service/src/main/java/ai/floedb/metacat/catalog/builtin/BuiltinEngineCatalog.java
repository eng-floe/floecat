package ai.floedb.metacat.catalog.builtin;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Immutable view of all builtin objects for a specific engine version.
 *
 * <p>The registry loads {@link BuiltinCatalogData} once per engine version, then materialises this
 * structure with convenient lookups (by name/signature) and a deterministic fingerprint so higher
 * layers can cache derived payloads safely.
 */
public final class BuiltinEngineCatalog {

  private final String engineVersion;
  private final String fingerprint;
  private final List<BuiltinFunctionDef> functions;
  private final Map<String, List<BuiltinFunctionDef>> functionsByName;
  private final List<BuiltinOperatorDef> operators;
  private final Map<String, BuiltinOperatorDef> operatorsByName;
  private final List<BuiltinTypeDef> types;
  private final Map<String, BuiltinTypeDef> typesByName;
  private final List<BuiltinCastDef> casts;
  private final Map<String, BuiltinCastDef> castsByKey;
  private final List<BuiltinCollationDef> collations;
  private final Map<String, BuiltinCollationDef> collationsByName;
  private final List<BuiltinAggregateDef> aggregates;
  private final Map<String, BuiltinAggregateDef> aggregatesByName;

  private BuiltinEngineCatalog(
      String engineVersion,
      String fingerprint,
      List<BuiltinFunctionDef> functions,
      Map<String, List<BuiltinFunctionDef>> functionsByName,
      List<BuiltinOperatorDef> operators,
      Map<String, BuiltinOperatorDef> operatorsByName,
      List<BuiltinTypeDef> types,
      Map<String, BuiltinTypeDef> typesByName,
      List<BuiltinCastDef> casts,
      Map<String, BuiltinCastDef> castsByKey,
      List<BuiltinCollationDef> collations,
      Map<String, BuiltinCollationDef> collationsByName,
      List<BuiltinAggregateDef> aggregates,
      Map<String, BuiltinAggregateDef> aggregatesByName) {
    this.engineVersion = engineVersion;
    this.fingerprint = fingerprint;
    this.functions = functions;
    this.functionsByName = functionsByName;
    this.operators = operators;
    this.operatorsByName = operatorsByName;
    this.types = types;
    this.typesByName = typesByName;
    this.casts = casts;
    this.castsByKey = castsByKey;
    this.collations = collations;
    this.collationsByName = collationsByName;
    this.aggregates = aggregates;
    this.aggregatesByName = aggregatesByName;
  }

  /** Builds a catalog snapshot from parsed builtin data. */
  public static BuiltinEngineCatalog from(String engineVersion, BuiltinCatalogData data) {
    List<BuiltinFunctionDef> functions = copy(data.functions());
    List<BuiltinOperatorDef> operators = copy(data.operators());
    List<BuiltinTypeDef> types = copy(data.types());
    List<BuiltinCastDef> casts = copy(data.casts());
    List<BuiltinCollationDef> collations = copy(data.collations());
    List<BuiltinAggregateDef> aggregates = copy(data.aggregates());

    String fingerprint = computeFingerprint(data);

    return new BuiltinEngineCatalog(
        engineVersion,
        fingerprint,
        functions,
        indexMulti(functions, BuiltinFunctionDef::name),
        operators,
        indexUnique(operators, BuiltinOperatorDef::name),
        types,
        indexUnique(types, BuiltinTypeDef::name),
        casts,
        indexUnique(casts, cast -> cast.sourceType() + "->" + cast.targetType()),
        collations,
        indexUnique(collations, BuiltinCollationDef::name),
        aggregates,
        indexUnique(aggregates, BuiltinAggregateDef::name));
  }

  public String engineVersion() {
    return engineVersion;
  }

  /** Stable fingerprint derived from the protobuf payload. */
  public String fingerprint() {
    return fingerprint;
  }

  public List<BuiltinFunctionDef> functions() {
    return functions;
  }

  /** Returns all overloads matching the provided name (may be empty). */
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

  public Optional<BuiltinCastDef> cast(String sourceType, String targetType) {
    return Optional.ofNullable(castsByKey.get(sourceType + "->" + targetType));
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

  private static String computeFingerprint(BuiltinCatalogData data) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] bytes = BuiltinCatalogProtoMapper.toProto(data).toByteArray();
      return HexFormat.of().formatHex(digest.digest(bytes));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 digest not available", e);
    }
  }
}

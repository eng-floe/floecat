# Error Messages Generator

This Maven plugin creates an error-message registry with a single source of truth derived from
`service/src/main/resources/errors_en.properties`.

It runs during the `generate-sources` phase (see `service/pom.xml`), validates the error-bundle layout, and emits
`ai.floedb.floecat.service.error.impl.GeneratedErrorMessages` under
`target/generated-sources/error-messages`.

The generated registry is used by the service at runtime to resolve error codes, message keys, and placeholders in a
strongly-typed and fully validated way.

## Validations

The plugin enforces the invariants required for safe automation and localization:

1. **Locale parity**  
   Every `errors_*.properties` file must contain *exactly* the same keys as the canonical English bundle
   (`errors_en.properties`). Missing or extra keys fail the build with a diff-style error.

2. **Suffix style & uniqueness**  
   Message keys must follow the `ErrorCode.suffix` format:
   - suffixes are dot-separated segments (e.g. `query.table.not.pinned`)
   - underscores are forbidden (to avoid ambiguous normalization)
   - each suffix must be globally unique across all error codes

   Any violation fails the build early, preventing runtime ambiguity.

3. **Error-code prefix validation**  
   Each key’s prefix (e.g. `MC_INVALID_ARGUMENT`) must match an entry in
   `floecat/common/common.proto`. Unknown or misspelled prefixes abort generation.

4. **Placeholder parity**  
   Placeholders inferred from `errors_en.properties` must be identical across all locales.
   This guarantees that translations cannot accidentally drop or rename placeholders such as
   `{table.id}` or `{snapshot_id}`.

5. **Deterministic, stable output**  
   - Keys are processed and emitted in a stable, sorted order.
   - Enum constant names are deterministically derived from suffixes.
   - If two suffixes would normalize to the same enum constant, generation fails fast with a clear
     error instead of emitting unstable or hashed identifiers.

## Usage

```bash
mvn -pl service generate-sources                 # regenerates GeneratedErrorMessages
mvn -pl tools/error-messages-generator test      # validates the mojo locally
```

The generated `MessageKey` enum exposes:

- `errorCode()`
- `suffix()`
- `fullKey()`
- `placeholders()`
- `bySuffix(...)` / `findBySuffix(...)`
- `byFullKey(...)` / `findByFullKey(...)`

This allows the service to continue emitting the same wire-level fields while benefiting from a
strongly-typed, compile-time-validated registry.

## Workflow

When adding or modifying an error message:

1. Update `errors_en.properties`.
2. Ensure all locale bundles add or update the identical key *with identical placeholders*.
3. Run `mvn -pl service generate-sources` and rebuild.

The build will fail with precise, actionable feedback if any invariant is violated.

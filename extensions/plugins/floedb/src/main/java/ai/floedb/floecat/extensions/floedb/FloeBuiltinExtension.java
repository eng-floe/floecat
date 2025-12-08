package ai.floedb.floecat.extensions.floedb;

import ai.floedb.floecat.catalog.builtin.registry.BuiltinCatalogData;
import ai.floedb.floecat.catalog.builtin.registry.BuiltinCatalogProtoMapper;
import ai.floedb.floecat.catalog.builtin.spi.EngineBuiltinExtension;
import ai.floedb.floecat.extensions.floedb.proto.*;
import ai.floedb.floecat.query.rpc.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.TextFormat;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Base class for Floe builtin extensions: loads human-friendly PBtxt files and rewrites
 * engine_specific blocks into payload bytes.
 *
 * <p>Floe-specific structures (floe_function, floe_type…) are embedded in PBtxt using readable
 * fields, then extracted from UnknownFieldSet, re-parsed into the correct Floe*Specific proto, and
 * serialized into payload bytes.
 */
public abstract class FloeBuiltinExtension implements EngineBuiltinExtension {

  @Override
  public final BuiltinCatalogData loadBuiltinCatalog() {
    BuiltinRegistry registry = loadFromResource(getResourcePath());
    BuiltinRegistry rewritten = rewriteFloeExtensions(registry);
    return BuiltinCatalogProtoMapper.fromProto(rewritten, engineKind());
  }

  /**
   * Returns the resource path to load (e.g., "/builtins/floedb.pbtxt"). Subclasses can override to
   * use different files.
   */
  protected String getResourcePath() {
    return "/builtins/" + engineKind() + ".pbtxt";
  }

  // ---------------------------------------------------------------------
  // PBtxt loader
  // ---------------------------------------------------------------------

  protected BuiltinRegistry loadFromResource(String resourcePath) {
    InputStream in = getClass().getResourceAsStream(resourcePath);
    if (in == null) {
      throw new IllegalStateException("Builtin file not found: " + resourcePath);
    }
    try (in) {
      // Register all Floe proto extensions so TextFormat parser can deserialize them
      ExtensionRegistry extensionRegistry = ExtensionRegistry.newInstance();
      EngineFloe.registerAllExtensions(extensionRegistry);

      BuiltinRegistry.Builder builder = BuiltinRegistry.newBuilder();
      var parser = TextFormat.Parser.newBuilder().setAllowUnknownFields(true).build();
      parser.merge(new InputStreamReader(in, StandardCharsets.UTF_8), extensionRegistry, builder);
      return builder.build();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to load builtin file: " + resourcePath, e);
    }
  }

  // ---------------------------------------------------------------------
  // Rewrite PBtxt engine_specific blocks → payload bytes
  // ---------------------------------------------------------------------

  protected BuiltinRegistry rewriteFloeExtensions(BuiltinRegistry in) {
    BuiltinRegistry.Builder out = in.toBuilder();

    // Functions
    out.clearFunctions();
    for (SqlFunction fn : in.getFunctionsList()) {
      SqlFunction.Builder fb = fn.toBuilder().clearEngineSpecific();
      for (EngineSpecific es : fn.getEngineSpecificList()) {
        fb.addEngineSpecific(convertRule(es));
      }
      out.addFunctions(fb);
    }

    // Operators
    out.clearOperators();
    for (SqlOperator op : in.getOperatorsList()) {
      SqlOperator.Builder ob = op.toBuilder().clearEngineSpecific();
      for (EngineSpecific es : op.getEngineSpecificList()) {
        ob.addEngineSpecific(convertRule(es));
      }
      out.addOperators(ob);
    }

    // Types
    out.clearTypes();
    for (SqlType ty : in.getTypesList()) {
      SqlType.Builder tb = ty.toBuilder().clearEngineSpecific();
      for (EngineSpecific es : ty.getEngineSpecificList()) {
        tb.addEngineSpecific(convertRule(es));
      }
      out.addTypes(tb);
    }

    // Casts
    out.clearCasts();
    for (SqlCast c : in.getCastsList()) {
      SqlCast.Builder cb = c.toBuilder().clearEngineSpecific();
      for (EngineSpecific es : c.getEngineSpecificList()) {
        cb.addEngineSpecific(convertRule(es));
      }
      out.addCasts(cb);
    }

    // Collations
    out.clearCollations();
    for (SqlCollation c : in.getCollationsList()) {
      SqlCollation.Builder cb = c.toBuilder().clearEngineSpecific();
      for (EngineSpecific es : c.getEngineSpecificList()) {
        cb.addEngineSpecific(convertRule(es));
      }
      out.addCollations(cb);
    }

    // Aggregates
    out.clearAggregates();
    for (SqlAggregate a : in.getAggregatesList()) {
      SqlAggregate.Builder ab = a.toBuilder().clearEngineSpecific();
      for (EngineSpecific es : a.getEngineSpecificList()) {
        ab.addEngineSpecific(convertRule(es));
      }
      out.addAggregates(ab);
    }

    return out.build();
  }

  // ---------------------------------------------------------------------
  // Convert readable PBtxt → opaque payload bytes
  // ---------------------------------------------------------------------

  protected EngineSpecific convertRule(EngineSpecific es) {

    // Already rewritten → nothing to do
    if (!es.getPayload().isEmpty()) {
      return es;
    }

    // Extract Floe extensions (proto2 extensions registered in ExtensionRegistry)
    // These are the proper way to handle structured Floe-specific data
    if (es.hasExtension(EngineFloe.floeFunction)) {
      FloeFunctionSpecific spec = es.getExtension(EngineFloe.floeFunction);
      return rewriteExtension(es, spec, "floe.function+proto");
    }
    if (es.hasExtension(EngineFloe.floeOperator)) {
      FloeOperatorSpecific spec = es.getExtension(EngineFloe.floeOperator);
      return rewriteExtension(es, spec, "floe.operator+proto");
    }
    if (es.hasExtension(EngineFloe.floeCast)) {
      FloeCastSpecific spec = es.getExtension(EngineFloe.floeCast);
      return rewriteExtension(es, spec, "floe.cast+proto");
    }
    if (es.hasExtension(EngineFloe.floeType)) {
      FloeTypeSpecific spec = es.getExtension(EngineFloe.floeType);
      return rewriteExtension(es, spec, "floe.type+proto");
    }
    if (es.hasExtension(EngineFloe.floeAggregate)) {
      FloeAggregateSpecific spec = es.getExtension(EngineFloe.floeAggregate);
      return rewriteExtension(es, spec, "floe.aggregate+proto");
    }
    if (es.hasExtension(EngineFloe.floeCollation)) {
      FloeCollationSpecific spec = es.getExtension(EngineFloe.floeCollation);
      return rewriteExtension(es, spec, "floe.collation+proto");
    }

    // No Floe extensions found - return unmodified
    return es;
  }

  /** Rewrite a proto2 extension into opaque payload bytes. */
  protected <T extends com.google.protobuf.Message> EngineSpecific rewriteExtension(
      EngineSpecific es, T extension, String type) {
    try {
      // Canonical binary serialization
      ByteString payload = extension.toByteString();

      return es.toBuilder()
          .setPayloadType(type)
          .setPayload(payload)
          .clearEngineKind() // engineKind comes from plugin
          .build();

    } catch (Exception e) {
      throw new IllegalStateException("Failed to rewrite extension payload type=" + type, e);
    }
  }

  /** Concrete implementation for the main FloeDB engine. */
  public static final class FloeDb extends FloeBuiltinExtension {
    @Override
    public String engineKind() {
      return "floedb";
    }
  }

  /** Concrete implementation for the demo FloeDB engine. */
  public static final class FloeDemo extends FloeBuiltinExtension {
    @Override
    public String engineKind() {
      return "floe-demo";
    }
  }
}

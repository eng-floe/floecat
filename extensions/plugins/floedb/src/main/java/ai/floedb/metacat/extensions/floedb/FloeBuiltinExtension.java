package ai.floedb.floecat.extensions.floedb;

import ai.floedb.floecat.catalog.builtin.BuiltinCatalogData;
import ai.floedb.floecat.catalog.builtin.BuiltinCatalogProtoMapper;
import ai.floedb.floecat.extensions.builtin.spi.EngineBuiltinExtension;
import ai.floedb.floecat.extensions.floedb.proto.*;
import ai.floedb.floecat.query.rpc.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import com.google.protobuf.UnknownFieldSet;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Floe builtin extension: loads human-friendly PBtxt files and rewrites engine_specific blocks into
 * payload bytes.
 *
 * <p>Floe-specific structures (floe_function, floe_type…) are embedded in PBtxt using readable
 * fields, then extracted from UnknownFieldSet, re-parsed into the correct Floe*Specific proto, and
 * serialized into payload bytes.
 */
public final class FloeBuiltinExtension implements EngineBuiltinExtension {

  @Override
  public String engineKind() {
    return "floedb";
  }

  @Override
  public BuiltinCatalogData loadBuiltinCatalog() {
    BuiltinRegistry registry = loadFromResource("/builtins/" + engineKind() + ".pbtxt");
    BuiltinRegistry rewritten = rewriteFloeExtensions(registry);
    return BuiltinCatalogProtoMapper.fromProto(rewritten, engineKind());
  }

  // ---------------------------------------------------------------------
  // PBtxt loader
  // ---------------------------------------------------------------------

  private BuiltinRegistry loadFromResource(String resourcePath) {
    try (InputStream in = getClass().getResourceAsStream(resourcePath)) {
      if (in == null) {
        throw new IllegalStateException("Builtin file not found: " + resourcePath);
      }
      BuiltinRegistry.Builder builder = BuiltinRegistry.newBuilder();
      TextFormat.getParser().merge(new InputStreamReader(in, StandardCharsets.UTF_8), builder);
      return builder.build();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to load builtin file: " + resourcePath, e);
    }
  }

  // ---------------------------------------------------------------------
  // Rewrite PBtxt engine_specific blocks → payload bytes
  // ---------------------------------------------------------------------

  private BuiltinRegistry rewriteFloeExtensions(BuiltinRegistry in) {
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

  private EngineSpecific convertRule(EngineSpecific es) {

    // Already rewritten → nothing to do
    if (!es.getPayload().isEmpty()) {
      return es;
    }

    UnknownFieldSet u = es.getUnknownFields();

    // The unknown field number (1–6) identifies which Floe proto is embedded
    if (u.hasField(1)) {
      return parseAndRewrite(
          es,
          "floe.function+proto",
          FloeFunctionSpecific.parser(),
          u.getField(1).getLengthDelimitedList().get(0));
    }
    if (u.hasField(2)) {
      return parseAndRewrite(
          es,
          "floe.operator+proto",
          FloeOperatorSpecific.parser(),
          u.getField(2).getLengthDelimitedList().get(0));
    }
    if (u.hasField(3)) {
      return parseAndRewrite(
          es,
          "floe.cast+proto",
          FloeCastSpecific.parser(),
          u.getField(3).getLengthDelimitedList().get(0));
    }
    if (u.hasField(4)) {
      return parseAndRewrite(
          es,
          "floe.type+proto",
          FloeTypeSpecific.parser(),
          u.getField(4).getLengthDelimitedList().get(0));
    }
    if (u.hasField(5)) {
      return parseAndRewrite(
          es,
          "floe.aggregate+proto",
          FloeAggregateSpecific.parser(),
          u.getField(5).getLengthDelimitedList().get(0));
    }
    if (u.hasField(6)) {
      return parseAndRewrite(
          es,
          "floe.collation+proto",
          FloeCollationSpecific.parser(),
          u.getField(6).getLengthDelimitedList().get(0));
    }

    return es; // no Floe-specific block
  }

  private <T extends com.google.protobuf.Message> EngineSpecific parseAndRewrite(
      EngineSpecific es, String type, com.google.protobuf.Parser<T> parser, ByteString raw) {

    try {
      // Parse the readable PBtxt block into the correct proto type
      T parsed = parser.parseFrom(raw);

      // Canonical binary serialization
      ByteString payload = parsed.toByteString();

      return es.toBuilder()
          .setPayloadType(type)
          .setPayload(payload)
          .clearEngineKind() // engineKind comes from plugin
          .build();

    } catch (Exception e) {
      throw new IllegalStateException("Failed to parse Floe-specific payload type=" + type, e);
    }
  }
}

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

package ai.floedb.floecat.extensions.floedb;

import static ai.floedb.floecat.extensions.floedb.utils.FloePayloads.*;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.extensions.floedb.proto.*;
import ai.floedb.floecat.extensions.floedb.sinks.FloeEngineSpecificDecorator;
import ai.floedb.floecat.extensions.floedb.utils.PayloadDescriptor;
import ai.floedb.floecat.query.rpc.*;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogProtoMapper;
import ai.floedb.floecat.systemcatalog.spi.EngineSystemCatalogExtension;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecorator;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import ai.floedb.floecat.systemcatalog.util.EngineContextNormalizer;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.TextFormat;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Base class for Floe builtin extensions: loads human-friendly PBtxt files and rewrites
 * engine_specific blocks into payload bytes.
 *
 * <p>Floe-specific structures (floe_function, floe_type…) are embedded in PBtxt using readable
 * fields, then extracted from UnknownFieldSet, re-parsed into the correct Floe*Specific proto, and
 * serialized into payload bytes.
 */
public abstract class FloeCatalogExtension implements EngineSystemCatalogExtension {

  @Override
  public final SystemCatalogData loadSystemCatalog() {
    BuiltinRegistry registry = loadFromResource(getResourcePath());
    BuiltinRegistry rewritten = rewriteFloeExtensions(registry);
    return SystemCatalogProtoMapper.fromProto(rewritten, engineKind());
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
      return rewriteExtension(es, es.getExtension(EngineFloe.floeFunction), FUNCTION);
    }

    if (es.hasExtension(EngineFloe.floeOperator)) {
      return rewriteExtension(es, es.getExtension(EngineFloe.floeOperator), OPERATOR);
    }

    if (es.hasExtension(EngineFloe.floeType)) {
      return rewriteExtension(es, es.getExtension(EngineFloe.floeType), TYPE);
    }

    if (es.hasExtension(EngineFloe.floeAggregate)) {
      return rewriteExtension(es, es.getExtension(EngineFloe.floeAggregate), AGGREGATE);
    }

    if (es.hasExtension(EngineFloe.floeCollation)) {
      return rewriteExtension(es, es.getExtension(EngineFloe.floeCollation), COLLATION);
    }

    if (es.hasExtension(EngineFloe.floeCast)) {
      return rewriteExtension(es, es.getExtension(EngineFloe.floeCast), CAST);
    }

    // No Floe extensions found - return unmodified
    return es;
  }

  /** Rewrite a proto2 extension into opaque payload bytes. */
  protected <T extends com.google.protobuf.Message> EngineSpecific rewriteExtension(
      EngineSpecific es, T extension, PayloadDescriptor<T> descriptor) {

    try {
      return es.toBuilder()
          .setPayloadType(descriptor.type())
          .setPayload(extension.toByteString())
          .clearEngineKind()
          .build();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to rewrite extension payload type=" + descriptor.type(), e);
    }
  }

  @Override
  public List<SystemObjectDef> definitions() {
    return List.of();
  }

  @Override
  public boolean supportsEngine(String engineKind) {
    return EngineContextNormalizer.normalizeEngineKind(engineKind)
        .equals(EngineContextNormalizer.normalizeEngineKind(this.engineKind()));
  }

  @Override
  public boolean supports(NameRef name, String engineKind) {
    if (!supportsEngine(engineKind) || name == null) {
      return false;
    }

    String requested = NameRefUtil.canonical(name);
    if (requested.isEmpty()) {
      return false;
    }

    return definitions().stream()
        .map(SystemObjectDef::name)
        .map(NameRefUtil::canonical)
        .anyMatch(requested::equals);
  }

  @Override
  public Optional<SystemObjectScanner> provide(
      String scannerId, String engineKind, String engineVersion) {
    return Optional.empty();
  }

  @Override
  public Optional<EngineMetadataDecorator> decorator() {
    return Optional.of(FloeEngineSpecificDecorator.INSTANCE);
  }

  /** Concrete implementation for the main FloeDB engine. */
  public static final class FloeDb extends FloeCatalogExtension {
    @Override
    public String engineKind() {
      return "floedb";
    }
  }

  /** Concrete implementation for the demo FloeDB engine. */
  public static final class FloeDemo extends FloeCatalogExtension {
    @Override
    public String engineKind() {
      return "floe-demo";
    }
  }
}

package ai.floedb.floecat.extensions.floedb.utils;

import ai.floedb.floecat.extensions.floedb.proto.FloeAggregateSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeCastSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeCollationSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeFunctionSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeOperatorSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeTypeSpecific;

/**
 * Central registry of Floe engine-specific payload descriptors.
 *
 * <p>This is the single source of truth for:
 *
 * <ul>
 *   <li>payload type strings
 *   <li>binary decoding logic
 *   <li>linking encoders ↔ decoders
 * </ul>
 *
 * Scanners and catalog extensions must reference these descriptors instead of hardcoding
 * payloadType strings.
 */
public final class FloePayloads {

  private FloePayloads() {}

  /** pg_proc / functions */
  public static final PayloadDescriptor<FloeFunctionSpecific> FUNCTION =
      PayloadDescriptor.of("floe.function+proto", FloeFunctionSpecific::parseFrom);

  /** pg_namespace */
  public static final PayloadDescriptor<FloeNamespaceSpecific> NAMESPACE =
      PayloadDescriptor.of("floe.namespace+proto", FloeNamespaceSpecific::parseFrom);

  /** pg_type */
  public static final PayloadDescriptor<FloeTypeSpecific> TYPE =
      PayloadDescriptor.of("floe.type+proto", FloeTypeSpecific::parseFrom);

  /** pg_operator */
  public static final PayloadDescriptor<FloeOperatorSpecific> OPERATOR =
      PayloadDescriptor.of("floe.operator+proto", FloeOperatorSpecific::parseFrom);

  /** pg_cast */
  public static final PayloadDescriptor<FloeCastSpecific> CAST =
      PayloadDescriptor.of("floe.cast+proto", FloeCastSpecific::parseFrom);

  /** pg_aggregate */
  public static final PayloadDescriptor<FloeAggregateSpecific> AGGREGATE =
      PayloadDescriptor.of("floe.aggregate+proto", FloeAggregateSpecific::parseFrom);

  /** pg_collation */
  public static final PayloadDescriptor<FloeCollationSpecific> COLLATION =
      PayloadDescriptor.of("floe.collation+proto", FloeCollationSpecific::parseFrom);

  public static final PayloadDescriptor<FloeRelationSpecific> RELATION =
      PayloadDescriptor.of("floe.relation+proto", FloeRelationSpecific::parseFrom);
}

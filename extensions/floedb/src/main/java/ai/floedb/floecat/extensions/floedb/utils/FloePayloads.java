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

package ai.floedb.floecat.extensions.floedb.utils;

import ai.floedb.floecat.extensions.floedb.proto.FloeAccessMethods;
import ai.floedb.floecat.extensions.floedb.proto.FloeAggregateSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeCastSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeCollationSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeColumnSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeFunctionSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeOperatorAccessMethods;
import ai.floedb.floecat.extensions.floedb.proto.FloeOperatorClasses;
import ai.floedb.floecat.extensions.floedb.proto.FloeOperatorFamilies;
import ai.floedb.floecat.extensions.floedb.proto.FloeOperatorSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeProcedureAccessMethods;
import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeTypePlanningSemantics;
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

  /** Floe type planning semantics to seed typcache. */
  public static final PayloadDescriptor<FloeTypePlanningSemantics> TYPE_PLANNING_SEMANTICS =
      PayloadDescriptor.of(
          "floe.type.planning_semantics+proto", FloeTypePlanningSemantics::parseFrom);

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

  /** pg_class / relations */
  public static final PayloadDescriptor<FloeRelationSpecific> RELATION =
      PayloadDescriptor.of("floe.relation+proto", FloeRelationSpecific::parseFrom);

  /** pg_attribute / columns */
  public static final PayloadDescriptor<FloeColumnSpecific> COLUMN =
      PayloadDescriptor.of("floe.column+proto", FloeColumnSpecific::parseFrom);

  /** pg_am / access method dictionary. */
  public static final PayloadDescriptor<FloeAccessMethods> ACCESS_METHODS =
      PayloadDescriptor.of("floe.access_methods+proto", FloeAccessMethods::parseFrom);

  /** pg_opfamily / operator family dictionary. */
  public static final PayloadDescriptor<FloeOperatorFamilies> OPERATOR_FAMILIES =
      PayloadDescriptor.of("floe.operator_families+proto", FloeOperatorFamilies::parseFrom);

  /** pg_opclass / operator class dictionary. */
  public static final PayloadDescriptor<FloeOperatorClasses> OPERATOR_CLASSES =
      PayloadDescriptor.of("floe.operator_classes+proto", FloeOperatorClasses::parseFrom);

  /** pg_amop / operator strategy dictionary. */
  public static final PayloadDescriptor<FloeOperatorAccessMethods> OPERATOR_ACCESS_METHODS =
      PayloadDescriptor.of(
          "floe.operator_access_methods+proto", FloeOperatorAccessMethods::parseFrom);

  /** pg_amproc / support procedure dictionary. */
  public static final PayloadDescriptor<FloeProcedureAccessMethods> PROCEDURE_ACCESS_METHODS =
      PayloadDescriptor.of(
          "floe.procedure_access_methods+proto", FloeProcedureAccessMethods::parseFrom);
}

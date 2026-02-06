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

import ai.floedb.floecat.extensions.floedb.proto.*;
import com.google.protobuf.Message;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class FloePayloads {
  private FloePayloads() {}

  public enum Descriptor {
    FUNCTION("floe.function+proto", FloeFunctionSpecific.class, FloeFunctionSpecific::parseFrom),
    NAMESPACE(
        "floe.namespace+proto", FloeNamespaceSpecific.class, FloeNamespaceSpecific::parseFrom),
    RELATION("floe.relation+proto", FloeRelationSpecific.class, FloeRelationSpecific::parseFrom),
    TYPE("floe.type+proto", FloeTypeSpecific.class, FloeTypeSpecific::parseFrom),
    COLLATION(
        "floe.collation+proto", FloeCollationSpecific.class, FloeCollationSpecific::parseFrom),
    OPERATOR("floe.operator+proto", FloeOperatorSpecific.class, FloeOperatorSpecific::parseFrom),
    CAST("floe.cast+proto", FloeCastSpecific.class, FloeCastSpecific::parseFrom),
    AGGREGATE(
        "floe.aggregate+proto", FloeAggregateSpecific.class, FloeAggregateSpecific::parseFrom),
    COLUMN("floe.column+proto", FloeColumnSpecific.class, FloeColumnSpecific::parseFrom),
    TYPE_PLANNING(
        "floe.type.planning_semantics+proto",
        FloeTypePlanningSemantics.class,
        FloeTypePlanningSemantics::parseFrom),
    ACCESS_METHODS(
        "floe.access_methods+proto", FloeAccessMethods.class, FloeAccessMethods::parseFrom),
    OPERATOR_FAMILIES(
        "floe.operator_families+proto",
        FloeOperatorFamilies.class,
        FloeOperatorFamilies::parseFrom),
    OPERATOR_CLASSES(
        "floe.operator_classes+proto", FloeOperatorClasses.class, FloeOperatorClasses::parseFrom),
    OPERATOR_ACCESS_METHODS(
        "floe.operator_access_methods+proto",
        FloeOperatorAccessMethods.class,
        FloeOperatorAccessMethods::parseFrom),
    PROCEDURE_ACCESS_METHODS(
        "floe.procedure_access_methods+proto",
        FloeProcedureAccessMethods.class,
        FloeProcedureAccessMethods::parseFrom);

    private final String type;
    private final Class<? extends Message> messageClass;
    private final ThrowingFunction<byte[], ? extends Message> decoder;

    Descriptor(
        String type,
        Class<? extends Message> messageClass,
        ThrowingFunction<byte[], ? extends Message> decoder) {
      this.type = type;
      this.messageClass = messageClass;
      this.decoder = decoder;
    }

    public String type() {
      return type;
    }

    public Class<? extends Message> messageClass() {
      return messageClass;
    }

    public Message decode(byte[] payload) throws Exception {
      return decoder.apply(payload);
    }
  }

  private static final Map<String, Descriptor> DESCRIPTORS =
      Arrays.stream(Descriptor.values())
          .collect(Collectors.toUnmodifiableMap(Descriptor::type, Function.identity()));

  public static Optional<Descriptor> descriptor(String type) {
    return Optional.ofNullable(DESCRIPTORS.get(type));
  }

  public static Descriptor requireDescriptor(String type) {
    return descriptor(type)
        .orElseThrow(
            () -> new IllegalArgumentException("Unknown Floe payload descriptor type: " + type));
  }
}

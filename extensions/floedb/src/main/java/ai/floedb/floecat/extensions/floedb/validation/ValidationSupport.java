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

package ai.floedb.floecat.extensions.floedb.validation;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.extensions.floedb.utils.FloePayloads;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.engine.VersionIntervals;
import ai.floedb.floecat.systemcatalog.util.EngineContextNormalizer;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.validation.Severity;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

final class ValidationSupport {
  private static final String GLOBAL_OID_CONFLICT = "floe.global_oid.conflict";

  private ValidationSupport() {}

  static void err(List<ValidationIssue> issues, String code, String ctx, Object... args) {
    issues.add(new ValidationIssue(code, Severity.ERROR, ctx, null, toArgList(args)));
  }

  static void err(
      List<ValidationIssue> issues,
      String code,
      String ctx,
      VersionIntervals.VersionInterval interval,
      Object... args) {
    issues.add(new ValidationIssue(code, Severity.ERROR, ctx, interval, toArgList(args)));
  }

  private static List<String> toArgList(Object[] args) {
    if (args == null || args.length == 0) {
      return List.of();
    }
    List<String> converted = new ArrayList<>(args.length);
    for (Object arg : args) {
      converted.add(String.valueOf(arg));
    }
    return List.copyOf(converted);
  }

  static String canonicalOrBlank(NameRef ref) {
    if (ref == null) {
      return "";
    }
    String c = NameRefUtil.canonical(ref);
    return c == null ? "" : c;
  }

  static boolean isBlank(String s) {
    return s == null || s.isBlank();
  }

  static boolean requirePositiveOid(
      List<ValidationIssue> issues, String code, String ctx, int oid) {
    if (oid <= 0) {
      err(issues, code, ctx, oid);
      return false;
    }
    return true;
  }

  static boolean requireNonBlankName(
      List<ValidationIssue> issues, String code, String ctx, NameRef name) {
    String canonical = canonicalOrBlank(name);
    if (canonical.isBlank()) {
      err(issues, code, ctx);
      return false;
    }
    return true;
  }

  static boolean requireKnownTypeOid(
      List<ValidationIssue> issues, String code, String ctx, String field, int oid, Lookup lookup) {
    if (oid <= 0) {
      return true;
    }
    if (!lookup.typesByOid().containsKey(oid)) {
      err(issues, code, ctx, field, oid);
      return false;
    }
    return true;
  }

  static String context(String kind, NameRef name) {
    if (name == null) {
      return kind + ":<null>";
    }
    String canonical = NameRefUtil.canonical(name);
    return kind + ":" + (canonical == null || canonical.isBlank() ? "<blank>" : canonical);
  }

  static String contextWithInterval(String baseContext, VersionIntervals.VersionInterval interval) {
    if (interval == null) {
      return baseContext;
    }
    return baseContext
        + "@["
        + formatBound(interval.min())
        + ","
        + formatBound(interval.max())
        + "]";
  }

  static <T extends Message> List<DecodedRule<T>> decodeAllPayloads(
      ValidationRunContext runContext,
      ValidationScope scope,
      List<EngineSpecificRule> rules,
      FloePayloads.Descriptor descriptor,
      Class<T> messageClass,
      String context,
      List<ValidationIssue> issues) {
    if (!descriptor.messageClass().equals(messageClass)) {
      throw new IllegalArgumentException(
          "Descriptor "
              + descriptor.name()
              + " requires "
              + descriptor.messageClass().getSimpleName()
              + ", got "
              + messageClass.getSimpleName());
    }
    Objects.requireNonNull(runContext, "runContext");
    if (rules == null || rules.isEmpty()) {
      return List.of();
    }

    List<EngineSpecificRule> applicable =
        rules.stream().filter(rule -> scope == null || scope.includes(rule)).toList();
    if (applicable.isEmpty()) {
      return List.of();
    }
    List<VersionIntervals.VersionInterval> existence = objectExistenceIntervals(applicable);
    if (existence.isEmpty()) {
      return List.of();
    }

    List<DecodedRule<T>> decoded = new ArrayList<>();
    List<VersionIntervals.VersionInterval> matchedIntervals = new ArrayList<>();
    Set<String> observedPayloadTypes = new LinkedHashSet<>();
    boolean matchedType = false;
    for (EngineSpecificRule rule : applicable) {
      if (!rule.payloadType().isBlank()) {
        observedPayloadTypes.add(rule.payloadType());
      }
      if (!rule.payloadTypeEquals(descriptor.type())) {
        continue;
      }
      matchedType = true;
      VersionIntervals.VersionInterval interval = VersionIntervals.VersionInterval.fromRule(rule);
      if (!rule.hasExtensionPayload()) {
        err(issues, "floe.payload.empty", context, descriptor.type(), formatInterval(interval));
        continue;
      }

      PayloadCacheKey cacheKey = new PayloadCacheKey(rule, descriptor.type());
      Message cached = runContext.get(cacheKey);
      if (cached != null) {
        if (!messageClass.isInstance(cached)) {
          err(
              issues,
              "floe.payload.decode_failed",
              context,
              descriptor.type(),
              formatInterval(interval),
              "cached-type-mismatch");
          continue;
        }
        @SuppressWarnings("unchecked")
        T typed = (T) cached;
        decoded.add(new DecodedRule<>(rule, interval, typed));
        matchedIntervals.add(interval);
        continue;
      }

      try {
        T payload = messageClass.cast(descriptor.decode(rule.extensionPayload()));
        runContext.put(cacheKey, payload);
        decoded.add(new DecodedRule<>(rule, interval, payload));
        matchedIntervals.add(interval);
      } catch (Exception e) {
        err(
            issues,
            "floe.payload.decode_failed",
            context,
            descriptor.type(),
            formatInterval(interval),
            e.getClass().getSimpleName());
      }
    }

    if (!matchedType) {
      String actual =
          observedPayloadTypes.isEmpty() ? "<none>" : String.join(",", observedPayloadTypes);
      err(issues, "floe.payload.type_missing", context, descriptor.type(), actual);
    } else if (!matchedIntervals.isEmpty()) {
      List<VersionIntervals.VersionInterval> coverage = VersionIntervals.union(matchedIntervals);
      for (VersionIntervals.VersionInterval interval : existence) {
        if (!VersionIntervals.covers(coverage, interval)) {
          err(
              issues,
              "floe.payload.coverage.missing",
              context,
              descriptor.type(),
              formatInterval(interval));
        }
      }
    }

    return List.copyOf(decoded);
  }

  static void trackGlobalOid(
      ValidationRunContext runContext,
      List<ValidationIssue> errors,
      String ctx,
      String domain,
      String identity,
      int oid,
      VersionIntervals.VersionInterval interval) {
    if (runContext == null || oid <= 0 || interval == null) {
      return;
    }
    runContext.trackGlobalOid(errors, ctx, domain, identity, oid, interval);
  }

  static void detectRuleOverlaps(
      List<? extends DecodedRule<?>> decodedRules,
      String payloadType,
      String ctx,
      List<ValidationIssue> issues) {
    if (decodedRules == null || decodedRules.size() < 2) {
      return;
    }

    Map<String, List<DecodedRule<?>>> buckets = new LinkedHashMap<>();
    List<DecodedRule<?>> wildcard = new ArrayList<>();
    for (DecodedRule<?> decoded : decodedRules) {
      String kind = EngineContextNormalizer.normalizeEngineKind(decoded.rule().engineKind());
      if (kind.isEmpty()) {
        wildcard.add(decoded);
      } else {
        buckets.computeIfAbsent(kind, k -> new ArrayList<>()).add(decoded);
      }
    }

    buckets.forEach((key, group) -> checkWithin(group, payloadType, ctx, key, issues));
    if (!wildcard.isEmpty()) {
      checkWithin(wildcard, payloadType, ctx, "<wildcard>", issues);
      for (Map.Entry<String, List<DecodedRule<?>>> entry : buckets.entrySet()) {
        checkBetween(wildcard, entry.getValue(), payloadType, ctx, entry.getKey(), issues);
      }
    }
  }

  private static void checkWithin(
      List<DecodedRule<?>> group,
      String payloadType,
      String ctx,
      String bucketKey,
      List<ValidationIssue> issues) {
    if (group == null || group.size() < 2) {
      return;
    }
    group.sort((a, b) -> VersionIntervals.compareBounds(a.interval().min(), b.interval().min()));
    for (int i = 0; i < group.size(); i++) {
      var base = group.get(i);
      for (int j = i + 1; j < group.size(); j++) {
        var next = group.get(j);
        if (VersionIntervals.compareBounds(base.interval().max(), next.interval().min()) < 0) {
          break;
        }
        if (VersionIntervals.overlaps(base.interval(), next.interval())) {
          reportOverlap(ctx, payloadType, bucketKey, base.interval(), next.interval(), issues);
        }
      }
    }
  }

  private static void checkBetween(
      List<DecodedRule<?>> wildcard,
      List<DecodedRule<?>> bucket,
      String payloadType,
      String ctx,
      String bucketKey,
      List<ValidationIssue> issues) {
    if (wildcard.isEmpty() || bucket.isEmpty()) {
      return;
    }
    for (DecodedRule<?> wild : wildcard) {
      for (DecodedRule<?> specific : bucket) {
        if (VersionIntervals.overlaps(wild.interval(), specific.interval())) {
          reportOverlap(ctx, payloadType, bucketKey, wild.interval(), specific.interval(), issues);
        }
      }
    }
  }

  private static void reportOverlap(
      String ctx,
      String payloadType,
      String bucketKey,
      VersionIntervals.VersionInterval a,
      VersionIntervals.VersionInterval b,
      List<ValidationIssue> issues) {
    ValidationSupport.err(
        issues,
        "floe.rules.overlap",
        ctx,
        payloadType,
        bucketKey,
        formatInterval(a),
        formatInterval(b));
  }

  static List<VersionIntervals.VersionInterval> objectExistenceIntervals(
      List<EngineSpecificRule> scopeRules) {
    if (scopeRules == null || scopeRules.isEmpty()) {
      return List.of();
    }
    return VersionIntervals.union(
        scopeRules.stream().map(VersionIntervals.VersionInterval::fromRule).toList());
  }

  static String formatInterval(VersionIntervals.VersionInterval interval) {
    if (interval == null) {
      return "<none>";
    }
    return "[" + formatBound(interval.min()) + "," + formatBound(interval.max()) + "]";
  }

  private static String formatBound(VersionIntervals.VersionBound bound) {
    if (bound == null) {
      return "<none>";
    }
    if (bound.isNegInf()) {
      return "-∞";
    }
    if (bound.isPosInf()) {
      return "+∞";
    }
    return bound.version();
  }

  private record PayloadCacheKey(EngineSpecificRule rule, String payloadType) {}

  static ValidationRunContext newRunContext() {
    return new ValidationRunContext();
  }

  static final class ValidationRunContext {
    private final Map<PayloadCacheKey, Message> cache = new HashMap<>();
    private final Map<Integer, List<GlobalOidEntry>> globalOids = new HashMap<>();

    @SuppressWarnings("unchecked")
    <T extends Message> T get(PayloadCacheKey key) {
      return (T) cache.get(key);
    }

    <T extends Message> void put(PayloadCacheKey key, T payload) {
      cache.put(key, payload);
    }

    void trackGlobalOid(
        List<ValidationIssue> issues,
        String ctx,
        String domain,
        String identity,
        int oid,
        VersionIntervals.VersionInterval interval) {
      if (issues == null
          || ctx == null
          || domain == null
          || identity == null
          || oid <= 0
          || interval == null) {
        return;
      }
      List<GlobalOidEntry> entries = globalOids.computeIfAbsent(oid, k -> new ArrayList<>());
      for (GlobalOidEntry entry : entries) {
        if (entry.domain.equals(domain) && entry.identity.equals(identity)) {
          return; // same identity can span multiple intervals
        }
        if (VersionIntervals.overlaps(entry.interval, interval)) {
          ValidationSupport.err(
              issues,
              GLOBAL_OID_CONFLICT,
              ctx,
              domain,
              entry.identity,
              ValidationSupport.formatInterval(entry.interval),
              identity,
              ValidationSupport.formatInterval(interval));
          return;
        }
      }
      entries.add(new GlobalOidEntry(domain, identity, interval));
    }

    private record GlobalOidEntry(
        String domain, String identity, VersionIntervals.VersionInterval interval) {}
  }

  static record DecodedRule<T>(
      EngineSpecificRule rule, VersionIntervals.VersionInterval interval, T payload) {}
}

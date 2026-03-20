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

package ai.floedb.floecat.client.cli;

import ai.floedb.floecat.common.rpc.PageRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/** Shared CLI argument parsing and pagination utilities used by all command support classes. */
final class CliArgs {

  private CliArgs() {}

  /** Returns all elements after the first, or an empty list if {@code list} has one or fewer. */
  static List<String> tail(List<String> list) {
    return list.size() <= 1 ? List.of() : list.subList(1, list.size());
  }

  /**
   * Splits a command-line string into tokens, respecting single-quoted, double-quoted, and
   * backslash-escaped segments. Throws if a quote is unclosed.
   */
  static List<String> tokenize(String line) {
    List<String> out = new ArrayList<>();
    StringBuilder cur = new StringBuilder();
    boolean inSingle = false;
    boolean inDouble = false;
    boolean escaping = false;

    for (int i = 0; i < line.length(); i++) {
      char ch = line.charAt(i);

      if (escaping) {
        cur.append('\\').append(ch);
        escaping = false;
        continue;
      }

      if (ch == '\\') {
        escaping = true;
        continue;
      }

      if (ch == '\'' && !inDouble) {
        inSingle = !inSingle;
        cur.append(ch);
        continue;
      }

      if (ch == '"' && !inSingle) {
        inDouble = !inDouble;
        cur.append(ch);
        continue;
      }

      if (!inSingle && !inDouble && Character.isWhitespace(ch)) {
        if (cur.length() > 0) {
          out.add(cur.toString());
          cur.setLength(0);
        }
        continue;
      }

      cur.append(ch);
    }

    if (inSingle || inDouble) {
      throw new IllegalArgumentException("Unclosed quote in command");
    }

    if (escaping) {
      cur.append('\\');
    }
    if (cur.length() > 0) {
      out.add(cur.toString());
    }
    return out;
  }

  /**
   * Returns the value following {@code flag} in {@code args}, converted via {@code fn}. Returns
   * {@code dflt} if the flag is absent or conversion fails.
   */
  static <T> T parseFlag(List<String> args, String flag, T dflt, Function<String, T> fn) {
    int i = args.indexOf(flag);
    if (i >= 0 && i + 1 < args.size()) {
      try {
        return fn.apply(args.get(i + 1));
      } catch (Exception ignore) {
      }
    }
    return dflt;
  }

  static String parseStringFlag(List<String> args, String flag, String defaultValue) {
    return parseFlag(args, flag, defaultValue, s -> s);
  }

  static int parseIntFlag(List<String> args, String flag, int defaultValue) {
    return parseFlag(args, flag, defaultValue, Integer::parseInt);
  }

  static long parseLongFlag(List<String> args, String flag, long defaultValue) {
    return parseFlag(args, flag, defaultValue, Long::parseLong);
  }

  static boolean hasFlag(List<String> args, String flag) {
    return args.contains(flag);
  }

  /**
   * Fetches all pages from a paginated gRPC call.
   *
   * @param pageSize number of items per page
   * @param fetch function that takes a {@link PageRequest} and returns a response
   * @param items extracts the item list from a response
   * @param next extracts the next-page token from a response (empty/null means last page)
   */
  static <T, R> List<T> collectPages(
      int pageSize,
      Function<PageRequest, R> fetch,
      Function<R, List<T>> items,
      Function<R, String> next) {

    List<T> all = new ArrayList<>();
    String token = "";
    do {
      R resp =
          fetch.apply(PageRequest.newBuilder().setPageSize(pageSize).setPageToken(token).build());
      all.addAll(items.apply(resp));
      token = Optional.ofNullable(next.apply(resp)).orElse("");
    } while (!token.isBlank());
    return all;
  }
}

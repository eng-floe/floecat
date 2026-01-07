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

grammar FQName;

@members {
    private void raiseUnclosedQuoteError() {
        throw new IllegalArgumentException("Unclosed quote in namespace path");
    }
}

fqn
  : segment (DOT+ segment)* EOF   // DOT+ collapses a..b, a...b to one delimiter
  ;

segment
  : QUOTED_DOUBLE
  | QUOTED_SINGLE
  | UNQUOTED
  ;

// Throw if we hit EOF before the closing quote.
UNCLOSED_DQ
  : '"' ( '\\' . | ~["\\\r\n] )* EOF
     { raiseUnclosedQuoteError(); }
  ;

UNCLOSED_SQ
  : '\'' ( '\\' . | ~['\\\r\n] )* EOF
     { raiseUnclosedQuoteError(); }
  ;

// A dot separates segments (unless inside quotes)
DOT : '.' ;

// Double-quoted string supports backslash escapes of any next char
QUOTED_DOUBLE
  : '"' ( '\\' . | ~["\\\r\n] )* '"'
  ;

// Single-quoted string supports backslash escapes of any next char
QUOTED_SINGLE
  : '\'' ( '\\' . | ~['\\\r\n] )* '\''
  ;

// Unquoted: no raw dot or quotes/whitespace; allow backslash-escaped char (incl '\.')
UNQUOTED
  : ( '\\' . | ~[.\t \r\n"'] )+
  ;

// Skip whitespace between segments
WS : [ \t\r\n]+ -> skip ;

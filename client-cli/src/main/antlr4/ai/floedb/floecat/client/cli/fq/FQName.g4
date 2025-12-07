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

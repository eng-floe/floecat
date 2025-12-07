grammar CsvList;

@members {
  private void raiseUnclosed() {
    throw new IllegalArgumentException("Unclosed quote in CSV list");
  }
}

/*
 * list := item (',' item)*
 * item := double-quoted | single-quoted | raw
 */
list : item (COMMA item)* EOF ;

item
  : DQSTR      #dqItem
  | SQSTR      #sqItem
  | RAW        #rawItem
  ;

COMMA : ',' ;

UNCLOSED_DQ
  : '"' ( '\\' . | ~["\\\r\n] )* EOF { raiseUnclosed(); }
  ;

UNCLOSED_SQ
  : '\'' ( '\\' . | ~['\\\r\n] )* EOF { raiseUnclosed(); }
  ;

// Double-quoted: supports \" and \\ and any other escaped char verbatim
DQSTR : '"' ( '\\' . | ~["\\\r\n] )* '"' ;

// Single-quoted: supports \' and \\ and any other escaped char verbatim
SQSTR : '\'' ( '\\' . | ~['\\\r\n] )* '\'' ;

// Raw: no unquoted comma or whitespace; allow backslash-escaped char (incl '\,' and '\\')
RAW   : ( '\\' . | ~[,\t \r\n] )+ ;

// Skip inter-item whitespace
WS : [ \t\r\n]+ -> skip ;

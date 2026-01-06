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

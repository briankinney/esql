grammar esql;


// TODO: case-insensitive keywords
SELECT: 'SELECT';
FROM: 'FROM';
MATCHES: 'MATCHES';
WHERE: 'WHERE';
LIMIT: 'LIMIT';
AND: 'AND';
OR: 'OR';
NOT: 'NOT';
ORDER: 'ORDER';
BY: 'BY';
ASC: 'ASC';
DESC: 'DESC';
LIKE: 'LIKE';
GROUP: 'GROUP';
SCORE: 'SCORE';
PAINLESS: 'PAINLESS';

search_query
    : SELECT path_spec
      FROM index_spec
      (WHERE filter_spec)?
      (GROUP BY aggregation_spec)?
      (ORDER BY sort_spec)?
      (LIMIT limit_spec)?
      ';'
    ;

path_spec
    : '*' // all
    | selected_formula (',' selected_formula)*
    ;

selected_formula
    : field
    | painless_script
    | aggregate_formula
    ;

painless_script
    : PAINLESS PAINLESS_SCRIPT_BODY
    ;

PAINLESS_SCRIPT_BODY
    : '`' .*? '`'  // TODO: script validation
    ;

aggregate_formula
    : function '(' field ')'
    | function '(' painless_script ')'
    ;

field: IDENTIFIER;

index_spec: IDENTIFIER;

filter_spec
    : leaf_query
    | '(' filter_spec ')'
    | NOT filter_spec
    | filter_spec AND filter_spec // boolean query
    | filter_spec OR filter_spec // dis max query
    ;

leaf_query
    : field COMPARATOR literal
    | field MATCHES STRING_LITERAL
//    TODO
//  | literal COMPARATOR field
//  | formula COMPARATOR formula
//  | formula BETWEEN formula AND formula
//  | field
    ;

formula
    : field
    | literal
//    TODO
//  | painless_script
    ;

function
    : IDENTIFIER
    ;

limit_spec
    : INTEGER_LITERAL
    ;

sort_spec
    : sort_atom (',' sort_atom)*
    ;

sort_atom
    : sort_term sort_direction
    ;

sort_term
    : field_ref
    | formula
    | SCORE
    ;

aggregation_spec
    : aggregation_term (',' aggregation_term)*
    ;

aggregation_term
    : field_ref
//    TODO
//  | field
//  | painless_script
    ;

field_ref
    : INTEGER_LITERAL
    ;

sort_direction
    : ASC
    | DESC
    ;

literal
    : STRING_LITERAL
    | INTEGER_LITERAL
    | NUMERIC_LITERAL
//    TODO
//  | TIMESTAMP_LITERAL
    ;

COMPARATOR
    : '='
    | '!='
    | '<'
    | '>'
    | '<='
    | '>='
//    TODO
//  | '~='
    ;

STRING_LITERAL: '\''.*?'\'';

INTEGER_LITERAL
    : '-'? ('0' .. '9') +
    ;

NUMERIC_LITERAL
    : '-'? ('0' .. '9') * '.' ('0' .. '9') +
    ;


IDENTIFIER
    : UNESCAPED_IDENTIFIER
    | '"' UNESCAPED_IDENTIFIER '"'
    ;

fragment UNESCAPED_IDENTIFIER
    : [-._a-zA-Z0-9]+
    ;

WS:
    [ \r\n\t]+ -> skip
    ;

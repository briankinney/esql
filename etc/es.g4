grammar es;

SELECT: 'SELECT';
FROM: 'FROM';
MATCHING: 'MATCHING';
WHERE: 'WHERE';
LIMIT: 'LIMIT';
AGGREGATE: 'AGGREGATE';
AND: 'AND';
OR: 'OR';
NOT: 'NOT';
ORDER: 'ORDER';
BY: 'BY';
ASC: 'ASC';
DESC: 'DESC';
LIKE: 'LIKE';

root
    : SELECT path_spec
      FROM index_spec
      (MATCHING query_spec)?
      (WHERE filter_spec)?
      (LIMIT limit_spec)?
      (ORDER BY sort_spec)?
      ';'
    ;

path_spec
    : '*' // all
    | '-' // none
    | field (',' field)*
    ;

field: IDENTIFIER;

index_spec: IDENTIFIER;

query_spec
    : query_criterion
    | query_spec AND query_spec
    | query_spec OR query_spec
    | '(' query_spec ')'
    | NOT query_spec
    ;

query_criterion
    : field COMPARATOR literal
    | literal COMPARATOR field
    ;

filter_spec
    : filter_criterion
    | filter_spec AND filter_spec
    | filter_spec OR filter_spec
    | '(' filter_spec ')'
    | NOT filter_spec
    ;

filter_criterion
    : field COMPARATOR literal
    | literal COMPARATOR field
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
    | field
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
    ;

COMPARATOR
    : '='
    | '!='
    | '<'
    | '>'
    | '<='
    | '>='
    | '~='
    ;

STRING_LITERAL: '\''.*?'\'';

INTEGER_LITERAL
    : ('0' .. '9') +
    ;

NUMERIC_LITERAL
    : ('0' .. '9') * '.' ('0' .. '9') +
    ;


IDENTIFIER: [_a-zA-Z0-9]+;

WS:
    [ \r\n\t]+ -> skip
    ;

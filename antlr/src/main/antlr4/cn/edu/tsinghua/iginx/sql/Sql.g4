/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
grammar Sql;

sqlStatement
   : statement ';' EOF
   ;

statement
   : INSERT INTO insertFullPathSpec VALUES insertValuesSpec # insertStatement
   | LOAD DATA importFileClause INTO (path tagList? (SET KEY keyName = stringLiteral)? | insertFullPathSpec) (AT keyBase = INT)? # insertFromFileStatement
   | DELETE FROM path (COMMA path)* whereClause? withClause? # deleteStatement
   | EXPLAIN? (LOGICAL | PHYSICAL)? cteClause? queryClause orderByClause? limitClause? exportFileClause? # selectStatement
   | COUNT POINTS # countPointsStatement
   | DELETE COLUMNS path (COMMA path)* withClause? # deleteColumnsStatement
   | CLEAR DATA # clearDataStatement
   | SHOW COLUMNS showColumnsOptions # showColumnsStatement
   | SHOW REPLICA NUMBER # showReplicationStatement
   | ADD STORAGEENGINE storageEngineSpec # addStorageEngineStatement
   | ALTER STORAGEENGINE engineId = INT WITH PARAMS params = stringLiteral # alterEngineStatement
   | SHOW CLUSTER INFO # showClusterInfoStatement
   | CREATE USER username = nodeName IDENTIFIED BY password = nodeName # createUserStatement
   | GRANT permissionSpec TO USER username = nodeName # grantUserStatement
   | ALTER USER username = nodeName IDENTIFIED BY password = nodeName # changePasswordStatement
   | DROP USER username = nodeName # dropUserStatement
   | SHOW USER userSpec? # showUserStatement
   | SHOW FUNCTIONS # showRegisterTaskStatement
   | CREATE FUNCTION udfType udfClassRef (COMMA (udfType)? udfClassRef)* IN filePath = stringLiteral # registerTaskStatement
   | DROP FUNCTION name = stringLiteral # dropTaskStatement
   | COMMIT TRANSFORM JOB filePath = stringLiteral # commitTransformJobStatement
   | SHOW TRANSFORM JOB STATUS jobId = INT # showJobStatusStatement
   | CANCEL TRANSFORM JOB jobId = INT # cancelJobStatement
   | SHOW (jobStatus)? TRANSFORM JOBS # showEligibleJobStatement
   | REMOVE STORAGEENGINE removedStorageEngine (COMMA removedStorageEngine)* # removeStorageEngineStatement
   | SET CONFIG configName = stringLiteral configValue = stringLiteral # setConfigStatement
   | SHOW CONFIG (configName = stringLiteral)? # showConfigStatement
   | SHOW SESSIONID # showSessionIDStatement
   | COMPACT # compactStatement
   | SHOW RULES # showRulesStatement
   | SET RULES ruleAssignment (COMMA ruleAssignment)* # setRulesStatement
   ;

udfClassRef
   : name = stringLiteral FROM className = stringLiteral
   ;

insertFullPathSpec
   : path tagList? insertColumnsSpec
   ;

showColumnsOptions
   : (path (COMMA path)*)? withClause? limitClause?
   ;

cteClause
   : WITH commonTableExpr (COMMA commonTableExpr)*
   ;

commonTableExpr
   : cteName (LR_BRACKET columnsList RR_BRACKET)? AS LR_BRACKET queryClause RR_BRACKET
   | cteName (LR_BRACKET columnsList RR_BRACKET)? AS LR_BRACKET queryClause orderByClause limitClause RR_BRACKET
   ;

ruleAssignment
   : ruleName = ID OPERATOR_EQ ruleValue = (ON | OFF)
   ;

cteName
   : ID
   ;

columnsList
   : cteColumn (COMMA cteColumn)*
   ;

cteColumn
   : ID
   ;

queryClause
   : LR_BRACKET inBracketQuery = queryClause orderByClause? limitClause? RR_BRACKET
   | select
   | leftQuery = queryClause INTERSECT (ALL | DISTINCT)? rightQuery = queryClause
   | leftQuery = queryClause (UNION | EXCEPT) (ALL | DISTINCT)? rightQuery = queryClause
   ;

select
   : selectClause (fromClause whereClause? withClause? specialClause?)?
   ;

selectClause
   : SELECT DISTINCT? selectSublist (COMMA selectSublist)*
   | SELECT VALUE2META LR_BRACKET queryClause orderByClause? limitClause? RR_BRACKET
   ;

selectSublist
   : KEY (asClause | asKeyClause)
   | (expression | sequence) (asClause | asKeyClause)?
   ;

sequence
   : SEQUENCE LR_BRACKET (start = constant COMMA increment = constant)? RR_BRACKET
   ;

asKeyClause
   : AS KEY
   ;

expression
   : LR_BRACKET inBracketExpr = expression RR_BRACKET
   | constant
   | function
   | path
   | (PLUS | MINUS) expr = expression
   | leftExpr = expression (STAR | DIV | MOD) rightExpr = expression
   | leftExpr = expression (PLUS | MINUS) rightExpr = expression
   | caseSpecification
   | subquery
   ;

function
   : functionName LR_BRACKET (ALL | DISTINCT)? expression (COMMA expression)* (COMMA param)* RR_BRACKET
   ;

param
   : key = ID OPERATOR_EQ value = constant
   | value = constant
   ;

functionName
   : ID
   | COUNT
   ;

caseSpecification
   : simpleCase
   | searchedCase
   ;

simpleCase
   : CASE expression simpleWhenClause (simpleWhenClause)* elseClause? END
   ;

simpleWhenClause
   : WHEN ((comparisonOperator? value = expression) | ((NOT | EXCLAMATION)? stringLikeOperator regex = stringLiteral)) THEN result = expression
   ;

searchedCase
   : CASE searchedWhenClause (searchedWhenClause)* elseClause? END
   ;

searchedWhenClause
   : WHEN condition = orExpression THEN result = expression
   ;

elseClause
   : ELSE expression
   ;

whereClause
   : WHERE orExpression
   ;

orExpression
   : andExpression (OPERATOR_OR andExpression)*
   ;

andExpression
   : predicate (OPERATOR_AND predicate)*
   ;

predicate
   : (KEY | path) comparisonOperator constant
   | constant comparisonOperator (KEY | path)
   | (path | functionName LR_BRACKET path RR_BRACKET) inOperator array
   | path comparisonOperator path
   | path (NOT | EXCLAMATION)? stringLikeOperator regex = stringLiteral
   | (NOT | EXCLAMATION)? LR_BRACKET orExpression RR_BRACKET
   | predicateWithSubquery
   | expression comparisonOperator expression
   ;

predicateWithSubquery
   : NOT? EXISTS subquery
   | (path | constant | expression) inOperator subquery
   | (path | constant | expression) comparisonOperator quantifier subquery
   | subquery comparisonOperator subquery
   | (path | constant | expression) comparisonOperator subquery
   | subquery comparisonOperator (path | constant | expression)
   ;

quantifier
   : all
   | some
   ;

array
   : LR_BRACKET (constant (COMMA constant)*)? RR_BRACKET
   ;

all
   : ALL
   ;

some
   : SOME
   | ANY
   ;

withClause
   : WITH orTagExpression
   | WITH_PRECISE orPreciseExpression
   | WITHOUT TAG
   ;

orTagExpression
   : andTagExpression (OPERATOR_OR andTagExpression)*
   ;

andTagExpression
   : tagExpression (OPERATOR_AND tagExpression)*
   ;

tagExpression
   : tagKey OPERATOR_EQ tagValue
   | LR_BRACKET orTagExpression RR_BRACKET
   ;

orPreciseExpression
   : andPreciseExpression (OPERATOR_OR andPreciseExpression)*
   ;

andPreciseExpression
   : preciseTagExpression (OPERATOR_AND preciseTagExpression)*
   ;

preciseTagExpression
   : tagKey OPERATOR_EQ tagValue
   ;

tagList
   : LS_BRACKET tagEquation (COMMA tagEquation)* RS_BRACKET
   ;

tagEquation
   : tagKey OPERATOR_EQ tagValue
   ;

tagKey
   : ID
   ;

tagValue
   : ID
   | STAR
   ;

fromClause
   : FROM tableReference joinPart*
   ;

joinPart
   : COMMA tableReference
   | CROSS JOIN tableReference
   | join tableReference (ON orExpression | USING (KEY | colList))?
   ;

tableReference
   : path asClause?
   | subquery asClause?
   | LR_BRACKET SHOW COLUMNS showColumnsOptions RR_BRACKET asClause?
   ;

subquery
   : LR_BRACKET queryClause orderByClause? limitClause? RR_BRACKET
   ;

colList
   : path (COMMA path)*
   ;

join
   : INNER? JOIN
   | (LEFT | RIGHT | FULL) OUTER? JOIN
   | NATURAL ((LEFT | RIGHT) OUTER?)? JOIN
   ;

specialClause
   : groupByClause havingClause?
   | downsampleClause
   ;

groupByClause
   : GROUP BY groupByItem (COMMA groupByItem)*
   ;

groupByItem
   : path
   | expression
   ;

havingClause
   : HAVING orExpression
   ;

orderByClause
   : ORDER BY ((KEY (DESC | ASC)?) | orderItem) (COMMA orderItem)*
   ;

orderItem
   : (path | expression) (DESC | ASC)?
   ;

downsampleClause
   : OVER WINDOW LR_BRACKET SIZE aggLen (IN timeInterval)? (SLIDE aggLen)? RR_BRACKET
   ;

aggLen
   : (TIME_WITH_UNIT | INT)
   ;

asClause
   : AS? ID
   ;

timeInterval
   : LS_BRACKET startTime = timeValue COMMA endTime = timeValue RR_BRACKET
   | LR_BRACKET startTime = timeValue COMMA endTime = timeValue RR_BRACKET
   | LS_BRACKET startTime = timeValue COMMA endTime = timeValue RS_BRACKET
   | LR_BRACKET startTime = timeValue COMMA endTime = timeValue RS_BRACKET
   ;

limitClause
   : LIMIT INT COMMA INT
   | LIMIT INT offsetClause?
   | offsetClause? LIMIT INT
   ;

offsetClause
   : OFFSET INT
   ;

exportFileClause
   : INTO OUTFILE (csvFile (WITH HEADER)? | streamFile)
   ;

importFileClause
   : FROM INFILE csvFile (SKIPPING HEADER)?
   ;

csvFile
   : filePath = stringLiteral AS CSV fieldsOption? linesOption?
   ;

streamFile
   : dirPath = stringLiteral AS STREAM
   ;

fieldsOption
   : FIELDS (TERMINATED BY fieldsTerminated = stringLiteral)? (OPTIONALLY? ENCLOSED BY enclosed = stringLiteral)? (ESCAPED BY escaped = stringLiteral)?
   ;

linesOption
   : LINES TERMINATED BY linesTerminated = stringLiteral
   ;

permissionSpec
   : permission (COMMA permission)*
   ;

userSpec
   : nodeName (COMMA nodeName)*
   ;

permission
   : READ
   | WRITE
   | ADMIN
   | CLUSTER
   ;

comparisonOperator
   : type = OPERATOR_GT
   | type = OPERATOR_GTE
   | type = OPERATOR_LT
   | type = OPERATOR_LTE
   | type = OPERATOR_EQ
   | type = OPERATOR_NEQ
   | type = OPERATOR_GT_AND
   | type = OPERATOR_GTE_AND
   | type = OPERATOR_LT_AND
   | type = OPERATOR_LTE_AND
   | type = OPERATOR_EQ_AND
   | type = OPERATOR_NEQ_AND
   | type = OPERATOR_GT_OR
   | type = OPERATOR_GTE_OR
   | type = OPERATOR_LT_OR
   | type = OPERATOR_LTE_OR
   | type = OPERATOR_EQ_OR
   | type = OPERATOR_NEQ_OR
   ;

stringLikeOperator
   : type = OPERATOR_LIKE
   | type = OPERATOR_LIKE_AND
   | type = OPERATOR_LIKE_OR
   ;

inOperator
   : type = IN
   | type = OPERATOR_IN_AND
   | type = OPERATOR_IN_OR
   | type = OPERATOR_NOT_IN
   | type = OPERATOR_NOT_IN_AND
   | type = OPERATOR_NOT_IN_OR
   ;

insertColumnsSpec
   : LR_BRACKET KEY (COMMA insertPath)+ RR_BRACKET
   ;

insertPath
   : path tagList?
   ;

insertValuesSpec
   : (COMMA? insertMultiValue)*
   | LR_BRACKET cteClause? queryClause RR_BRACKET (TIME_OFFSET OPERATOR_EQ INT)?
   ;

insertMultiValue
   : LR_BRACKET timeValue (COMMA constant)+ RR_BRACKET
   ;

storageEngineSpec
   : (COMMA? storageEngine)+
   ;

storageEngine
   : LR_BRACKET ip = stringLiteral COMMA port = INT COMMA engineType = stringLiteral COMMA extra = stringLiteral RR_BRACKET
   ;

timeValue
   : dateFormat
   | dateExpression
   | INT
   | MINUS? INF
   ;

path
   : nodeName (DOT nodeName)*
   ;

udfType
   : UDAF
   | UDTF
   | UDSF
   | TRANSFORM
   ;

jobStatus
   : UNKNOWN
   | FINISHED
   | CREATED
   | IDLE
   | RUNNING
   | PARTIALLY_FAILING
   | PARTIALLY_FAILED
   | FAILING
   | FAILED
   | CLOSING
   | CLOSED
   ;

nodeName
   : ID
   | STAR
   | BACK_QUOTE_STRING_LITERAL_NOT_EMPTY
   | keyWords
   ;

keyWords
   : INSERT
   | DELETE
   | SELECT
   | SHOW
   | INTO
   | WHERE
   | FROM
   | BY
   | LIMIT
   | OFFSET
   | TIME
   | KEY
   | TIMESTAMP
   | GROUP
   | ORDER
   | HAVING
   | AGG
   | ADD
   | VALUE
   | VALUES
   | NOW
   | COUNT
   | CLEAR
   | DESC
   | ASC
   | STORAGEENGINE
   | POINTS
   | DATA
   | REPLICA
   | USER
   | PASSWORD
   | CLUSTER
   | ADMIN
   | WRITE
   | READ
   | DROP
   | CREATE
   | FUNCTION
   | FUNCTIONS
   | COMMIT
   | JOB
   | JOBS
   | STATUS
   | AS
   | udfType
   | jobStatus
   | ALTER
   | PARAMS
   | WITH
   | WITHOUT
   | TAG
   | WITH_PRECISE
   | TIME_OFFSET
   | CANCEL
   | INNER
   | OUTER
   | CROSS
   | NATURAL
   | LEFT
   | RIGHT
   | FULL
   | JOIN
   | ON
   | USING
   | OVER
   | RANGE
   | STEP
   | REMOVE
   | COMPACT
   | EXPLAIN
   | LOGICAL
   | PHYSICAL
   | SET
   | CONFIG
   | SESSIONID
   | COLUMNS
   | INTERSECT
   | UNION
   | EXCEPT
   | DISTINCT
   | OUTFILE
   | INFILE
   | CSV
   | STREAM
   | FIELDS
   | TERMINATED
   | OPTIONALLY
   | ENCLOSED
   | LINES
   | SKIPPING
   | HEADER
   | LOAD
   | VALUE2META
   | WINDOW
   | SIZE
   | SLIDE
   | CASE
   | WHEN
   | THEN
   | ELSE
   | END
   ;

dateFormat
   : DATETIME
   | TIME_WITH_UNIT
   | NOW LR_BRACKET RR_BRACKET
   ;

constant
   : dateExpression
   | MINUS? realLiteral // double
   | MINUS? INT // long
   | stringLiteral
   | booleanClause
   | NaN
   | NULL
   ;

booleanClause
   : TRUE
   | FALSE
   ;

dateExpression
   : dateFormat ((PLUS | MINUS) TIME_WITH_UNIT)*
   ;

realLiteral
   : INT DOT (INT | EXPONENT)?
   | DOT (INT | EXPONENT)
   | EXPONENT
   ;

removedStorageEngine
   : LR_BRACKET ip = stringLiteral COMMA port = INT COMMA schemaPrefix = stringLiteral COMMA dataPrefix = stringLiteral RR_BRACKET
   ;
   //============================
   
   // Start of the keywords list
   
   //============================
   
ALTER
   : A L T E R
   ;

PARAMS
   : P A R A M S
   ;

INSERT
   : I N S E R T
   ;

DELETE
   : D E L E T E
   ;

SELECT
   : S E L E C T
   ;

GRANT
   : G R A N T
   ;

USER
   : U S E R
   ;

PASSWORD
   : P A S S W O R D
   ;

SHOW
   : S H O W
   ;

REPLICA
   : R E P L I C A
   ;

NUMBER
   : N U M B E R
   ;

CLUSTER
   : C L U S T E R
   ;

ADMIN
   : A D M I N
   ;

READ
   : R E A D
   ;

WRITE
   : W R I T E
   ;

INFO
   : I N F O
   ;

WHERE
   : W H E R E
   ;

IDENTIFIED
   : I D E N T I F I E D
   ;

IN
   : I N
   ;

TO
   : T O
   ;

INTO
   : I N T O
   ;

FOR
   : F O R
   ;

FROM
   : F R O M
   ;

TIMESTAMP
   : T I M E S T A M P
   ;

GROUP
   : G R O U P
   ;

ORDER
   : O R D E R
   ;

HAVING
   : H A V I N G
   ;

AGG
   : A G G
   ;

BY
   : B Y
   ;

VALUE
   : V A L U E
   ;

VALUES
   : V A L U E S
   ;

NOW
   : N O W
   ;

TIME
   : T I M E
   ;

KEY
   : K E Y
   ;

TRUE
   : T R U E
   ;

FALSE
   : F A L S E
   ;

NULL
   : N U L L
   ;

COUNT
   : C O U N T
   ;

LIMIT
   : L I M I T
   ;

OFFSET
   : O F F S E T
   ;

DATA
   : D A T A
   ;

ADD
   : A D D
   ;

UPDATE
   : U P D A T E
   ;

RULES
   : R U L E S
   ;

STORAGEENGINE
   : S T O R A G E E N G I N E
   ;

POINTS
   : P O I N T S
   ;

CLEAR
   : C L E A R
   ;

DESC
   : D E S C
   ;

ASC
   : A S C
   ;

DROP
   : D R O P
   ;

COMMIT
   : C O M M I T
   ;

TRANSFORM
   : T R A N S F O R M
   ;

JOB
   : J O B
   ;

JOBS
   : J O B S
   ;

STATUS
   : S T A T U S
   ;

AS
   : A S
   ;

UDAF
   : U D A F
   ;

UDTF
   : U D T F
   ;

UDSF
   : U D S F
   ;

WITH
   : W I T H
   ;

WITHOUT
   : W I T H O U T
   ;

TAG
   : T A G
   ;

WITH_PRECISE
   : W I T H '_' P R E C I S E
   ;

TIME_OFFSET
   : T I M E '_' O F F S E T
   ;

CANCEL
   : C A N C E L
   ;

UNKNOWN
   : U N K N O W N
   ;

FINISHED
   : F I N I S H E D
   ;

CREATE
   : C R E A T E
   ;

FUNCTION
   : F U N C T I O N
   ;

FUNCTIONS
   : F U N C T I O N S
   ;

CREATED
   : C R E A T E D
   ;

IDLE
   : I D L E
   ;

RUNNING
   : R U N N I N G
   ;

PARTIALLY_FAILING
   : P A R T I A L L Y UNDERLINE F A I L I N G
   ;

PARTIALLY_FAILED
   : P A R T I A L L Y UNDERLINE F A I L E D
   ;

FAILING
   : F A I L I N G
   ;

FAILED
   : F A I L E D
   ;

CLOSING
   : C L O S I N G
   ;

CLOSED
   : C L O S E D
   ;

INNER
   : I N N E R
   ;

OUTER
   : O U T E R
   ;

CROSS
   : C R O S S
   ;

NATURAL
   : N A T U R A L
   ;

LEFT
   : L E F T
   ;

RIGHT
   : R I G H T
   ;

FULL
   : F U L L
   ;

JOIN
   : J O I N
   ;

ON
   : O N
   ;

OFF
   : O F F
   ;

USING
   : U S I N G
   ;

OVER
   : O V E R
   ;

RANGE
   : R A N G E
   ;

STEP
   : S T E P
   ;

REMOVE
   : R E M O V E
   ;

COMPACT
   : C O M P A C T
   ;

EXPLAIN
   : E X P L A I N
   ;

LOGICAL
   : L O G I C A L
   ;

PHYSICAL
   : P H Y S I C A L
   ;

EXISTS
   : E X I S T S
   ;

SOME
   : S O M E
   ;

ANY
   : A N Y
   ;

ALL
   : A L L
   ;

SET
   : S E T
   ;

CONFIG
   : C O N F I G
   ;

SESSIONID
   : S E S S I O N I D
   ;

COLUMNS
   : C O L U M N S
   ;

INTERSECT
   : I N T E R S E C T
   ;

UNION
   : U N I O N
   ;

EXCEPT
   : E X C E P T
   ;

DISTINCT
   : D I S T I N C T
   ;

OUTFILE
   : O U T F I L E
   ;

INFILE
   : I N F I L E
   ;

CSV
   : C S V
   ;

AT
   : A T
   ;

STREAM
   : S T R E A M
   ;

FIELDS
   : F I E L D S
   ;

TERMINATED
   : T E R M I N A T E D
   ;

OPTIONALLY
   : O P T I O N A L L Y
   ;

ENCLOSED
   : E N C L O S E D
   ;

ESCAPED
   : E S C A P E D
   ;

LINES
   : L I N E S
   ;

SKIPPING
   : S K I P P I N G
   ;

HEADER
   : H E A D E R
   ;

LOAD
   : L O A D
   ;

WINDOW
   : W I N D O W
   ;

SIZE
   : S I Z E
   ;

SLIDE
   : S L I D E
   ;

VALUE2META
   : V A L U E INT_2 M E T A
   ;

CASE
   : C A S E
   ;

WHEN
   : W H E N
   ;

THEN
   : T H E N
   ;

ELSE
   : E L S E
   ;

END
   : E N D
   ;

SEQUENCE
   : S E Q U E N C E
   ;

NOT
   : N O T
   ;
   //============================
   
   // End of the keywords list
   
   //============================
   
COMMA
   : ','
   ;

STAR
   : '*'
   ;

OPERATOR_EQ
   : '='
   | '=='
   ;

OPERATOR_GT
   : '>'
   ;

OPERATOR_GTE
   : '>='
   ;

OPERATOR_LT
   : '<'
   ;

OPERATOR_LTE
   : '<='
   ;

OPERATOR_NEQ
   : '!='
   | '<>'
   ;

OPERATOR_EQ_AND
   : '&' OPERATOR_EQ
   ;

OPERATOR_GT_AND
   : '&' OPERATOR_GT
   ;

OPERATOR_GTE_AND
   : '&' OPERATOR_GTE
   ;

OPERATOR_LT_AND
   : '&' OPERATOR_LT
   ;

OPERATOR_LTE_AND
   : '&' OPERATOR_LTE
   ;

OPERATOR_NEQ_AND
   : '&' OPERATOR_NEQ
   ;

OPERATOR_EQ_OR
   : '|' OPERATOR_EQ
   ;

OPERATOR_GT_OR
   : '|' OPERATOR_GT
   ;

OPERATOR_GTE_OR
   : '|' OPERATOR_GTE
   ;

OPERATOR_LT_OR
   : '|' OPERATOR_LT
   ;

OPERATOR_LTE_OR
   : '|' OPERATOR_LTE
   ;

OPERATOR_NEQ_OR
   : '|' OPERATOR_NEQ
   ;

OPERATOR_LIKE
   : L I K E
   ;

OPERATOR_LIKE_AND
   : '&' L I K E
   ;

OPERATOR_LIKE_OR
   : '|' L I K E
   ;

OPERATOR_AND
   : A N D
   | '&'
   | '&&'
   ;

OPERATOR_OR
   : O R
   | '|'
   | '||'
   ;

EXCLAMATION
   : '!'
   ;

OPERATOR_CONTAINS
   : C O N T A I N S
   ;

OPERATOR_NOT_IN
   : N O T WS IN
   ;

OPERATOR_IN_AND
   : '&' IN
   ;

OPERATOR_IN_OR
   : '|' IN
   ;

OPERATOR_NOT_IN_AND
   : '&' OPERATOR_NOT_IN
   ;

OPERATOR_NOT_IN_OR
   : '|' OPERATOR_NOT_IN
   ;

MINUS
   : '-'
   ;

PLUS
   : '+'
   ;

DIV
   : '/'
   ;

MOD
   : '%'
   ;

DOT
   : '.'
   ;

LR_BRACKET
   : '('
   ;

RR_BRACKET
   : ')'
   ;

LS_BRACKET
   : '['
   ;

RS_BRACKET
   : ']'
   ;

L_BRACKET
   : '{'
   ;

R_BRACKET
   : '}'
   ;

UNDERLINE
   : '_'
   ;

NaN
   : 'NaN'
   ;

BACK_QUOTE
   : '`'
   ;

INF
   : I N F
   ;

stringLiteral
   : SINGLE_QUOTE_STRING_LITERAL
   | DOUBLE_QUOTE_STRING_LITERAL
   ;

INT
   : [0-9]+
   ;

INT_2
   : [2]
   ;

EXPONENT
   : INT ('e' | 'E') ('+' | '-')? INT
   ;

TIME_WITH_UNIT
   : (INT+ (Y | M O | W | D | H | M | S | M S | U S | N S))+
   ;

DATETIME
   : INT ('-' | '/' | '.') INT ('-' | '/' | '.') INT ((T | WS) INT ':' INT ':' INT (DOT INT)? (('+' | '-') INT ':' INT)?)?
   ;

/** Allow unicode rule/token names */ ID
   : NAME_CHAR+
   ;

fragment NAME_CHAR
   : 'A' .. 'Z'
   | 'a' .. 'z'
   | '0' .. '9'
   | '_'
   | '@'
   | '#'
   | ':'
   | '$'
   | '{'
   | '}'
   | '~'
   | '^'
   | '\\'
   | CN_CHAR
   ;

fragment CN_CHAR
   : '\u2E86' .. '\u9FFF'
   ;

BACK_QUOTE_STRING_LITERAL_NOT_EMPTY
   : BACK_QUOTE ('\\' . | ~ '"')+? BACK_QUOTE
   ;

DOUBLE_QUOTE_STRING_LITERAL
   : '"' ('\\' . | ~ '"')*? '"'
   ;

SINGLE_QUOTE_STRING_LITERAL
   : '\'' ('\\' . | ~ '\'')*? '\''
   ;
   //Characters and write it this way for case sensitivity
   
fragment A
   : 'a'
   | 'A'
   ;

fragment B
   : 'b'
   | 'B'
   ;

fragment C
   : 'c'
   | 'C'
   ;

fragment D
   : 'd'
   | 'D'
   ;

fragment E
   : 'e'
   | 'E'
   ;

fragment F
   : 'f'
   | 'F'
   ;

fragment G
   : 'g'
   | 'G'
   ;

fragment H
   : 'h'
   | 'H'
   ;

fragment I
   : 'i'
   | 'I'
   ;

fragment J
   : 'j'
   | 'J'
   ;

fragment K
   : 'k'
   | 'K'
   ;

fragment L
   : 'l'
   | 'L'
   ;

fragment M
   : 'm'
   | 'M'
   ;

fragment N
   : 'n'
   | 'N'
   ;

fragment O
   : 'o'
   | 'O'
   ;

fragment P
   : 'p'
   | 'P'
   ;

fragment Q
   : 'q'
   | 'Q'
   ;

fragment R
   : 'r'
   | 'R'
   ;

fragment S
   : 's'
   | 'S'
   ;

fragment T
   : 't'
   | 'T'
   ;

fragment U
   : 'u'
   | 'U'
   ;

fragment V
   : 'v'
   | 'V'
   ;

fragment W
   : 'w'
   | 'W'
   ;

fragment X
   : 'x'
   | 'X'
   ;

fragment Y
   : 'y'
   | 'Y'
   ;

fragment Z
   : 'z'
   | 'Z'
   ;

SIMPLE_COMMENT
   : '--' ~ [\r\n]* '\r'? '\n'? -> channel (HIDDEN)
   ;

BRACKETED_COMMENT
   : '/*' .*? '*/' -> channel (HIDDEN)
   ;

WS
   : [ \r\n\t]+ -> channel (HIDDEN)
   ;


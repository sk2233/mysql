/*
@author: sk
@date: 2024/8/7
*/
package main

const (
	// DDL
	CREATE = "CREATE"
	//DROP   = "DROP"
	TABLE = "TABLE"
	INDEX = "INDEX"
	// other
	//EXPLAIN = "EXPLAIN"
	// select
	SELECT   = "SELECT"
	STAR     = "STAR" // *
	FROM     = "FROM"
	WHERE    = "WHERE"
	GROUP    = "GROUP"
	ORDER    = "ORDER"
	BY       = "BY"
	ASC      = "ASC"
	DESC     = "DESC"
	JOIN     = "JOIN"
	DISTINCT = "DISTINCT"
	LIMIT    = "LIMIT"
	OFFSET   = "OFFSET"
	//LEFT   = "LEFT"
	//RIGHT  = "RIGHT"
	//INNER  = "INNER"
	ON = "ON"
	// DML
	INSERT = "INSERT"
	INTO   = "INTO"
	VALUES = "VALUES"
	DELETE = "DELETE"
	UPDATE = "UPDATE"
	SET    = "SET"
	// 标点符号
	DOT    = "DOT"    // .
	COMMA  = "COMMA"  // ,
	LPAREN = "LPAREN" // (
	RPAREN = "RPAREN" // )
	// operator
	EQ  = "EQ" // =
	NE  = "NE" // !=
	GT  = "GT" // >
	GE  = "GE" // >=
	LT  = "LT" // <
	LE  = "LE" // <=
	AND = "AND"
	OR  = "OR"
	//NOT = "NOT"  暂时先不支持一元操作符
	// data type
	ID = "ID" // wsws2233 变量名称
	// 支持的数据类型 其中 INT FLOAT 不仅是数据类型还是关键字 VARCHAR TEXT 指定的数据类型都是 STR
	INT     = "INT" // 2233
	FLOAT   = "FLOAT"
	STR     = "STR" // '你好'
	VARCHAR = "VARCHAR"
	TEXT    = "TEXT"
	//NULL = "NULL"
	EOF = "EOF" // 结束标记
)

var (
	Keywords = map[string]string{
		"CREATE": CREATE,
		//"DROP":    DROP,
		"TABLE": TABLE,
		"INDEX": INDEX,
		//"EXPLAIN": EXPLAIN,
		"SELECT":   SELECT,
		"FROM":     FROM,
		"WHERE":    WHERE,
		"GROUP":    GROUP,
		"ORDER":    ORDER,
		"BY":       BY,
		"ASC":      ASC,
		"DESC":     DESC,
		"JOIN":     JOIN,
		"DISTINCT": DISTINCT,
		"LIMIT":    LIMIT,
		"OFFSET":   OFFSET,
		//"LEFT":   LEFT,
		//"RIGHT":  RIGHT,
		//"INNER":  INNER,
		"ON":     ON,
		"INSERT": INSERT,
		"INTO":   INTO,
		"VALUES": VALUES,
		"DELETE": DELETE,
		"UPDATE": UPDATE,
		"SET":    SET,
		"AND":    AND,
		"OR":     OR,
		//"NULL": NULL,
		"INT":     INT,
		"FLOAT":   FLOAT,
		"VARCHAR": VARCHAR,
		"TEXT":    TEXT,
	}
)

type Token struct {
	Type  string
	Value string
}

func NewToken(type0 string, value string) *Token {
	return &Token{Type: type0, Value: value}
}

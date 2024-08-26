/*
@author: sk
@date: 2024/8/8
*/
package main

// 所有 Column 字段不直接使用 string 使用 IDNode 主要方便后续使用

type INode interface {
}

type IDNode struct {
	Value string // 关键字 & 字段
}

type StarNode struct { // *
}

type ImmNode struct { // '你好'  2332  22.33 等字面量
	Value string
}

type FuncNode struct {
	FuncName string
	Params   []INode // 可以是 IDNode ImmNode  FuncNode  ExprNode
}

type ExprNode struct { // 只支持一些简单的 二元条件
	Left     INode // 可以是  IDNode  ImmNode  FuncNode  ExprNode
	Right    INode
	Operator string // 只能是一些关键字
}

type OrderNode struct {
	Field *IDNode
	Desc  bool
}

type LimitNode struct {
	Limit  int
	Offset int
}

type JoinNode struct {
	Table     string
	Condition *ExprNode // 必须有 on 必须有条件
}

type SelectNode struct {
	Fields   []INode   // 可以是 IDNode  StarNode  FuncNode  ImmNode
	Distinct []*IDNode // 只支持字段名称
	From     string    // 仅支持表名
	Join     *JoinNode // 关联查询
	Where    *ExprNode // 条件
	Groups   []*IDNode // 仅支持字段名称 相关聚合函数在字段中
	Orders   []*OrderNode
	Limit    *LimitNode
}

type SetNode struct {
	Field *IDNode
	Value INode // 可以是 IDNode ImmNode FuncNode
}

type UpdateNode struct {
	Table string
	Sets  []*SetNode
	Where *ExprNode // 条件
}

type InsertNode struct {
	Table  string
	Values [][]*Value
}

type DeleteNode struct {
	Table string
	Where *ExprNode // 条件
}

type ColumnNode struct {
	Name *IDNode
	Type string
	Len  int64
}

type CreateTableNode struct { // 创建表结构节点
	Table   string
	Columns []*ColumnNode
}

type CreateIndexNode struct { // 创建索引节点
	Index   string
	Table   string
	Columns []*IDNode
}

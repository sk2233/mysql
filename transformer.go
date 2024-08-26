/*
@author: sk
@date: 2024/8/25
*/
package main

import "fmt"

type Transformer struct {
	Node INode
}

func NewTransformer(node INode) *Transformer {
	return &Transformer{Node: node}
}

func (t *Transformer) Transform() IOperator {
	switch target := t.Node.(type) {
	case *InsertNode:
		return t.transformInsert(target)
	case *DeleteNode:
		return t.transformDelete(target)
	case *UpdateNode:
		return t.transformUpdate(target)
	case *SelectNode:
		return t.transformSelect(target)
	case *CreateTableNode:
		return t.transformCreateTable(target)
	case *CreateIndexNode:
		return t.transformCreateIndex(target)
	default:
		panic(fmt.Sprintf("unknown node type: %T", t.Node))
	}
}

func (t *Transformer) transformCreateIndex(node *CreateIndexNode) IOperator {
	return nil
}

func (t *Transformer) transformCreateTable(node *CreateTableNode) IOperator {
	return nil
}

func (t *Transformer) transformSelect(node *SelectNode) IOperator {
	// 逻辑转换，语句重写  带有 count() 的非聚合 sql 需要转换为聚合 sql 重写需要对字段做处理

	// 物理算子转换
	return nil
}

func (t *Transformer) transformUpdate(node *UpdateNode) IOperator {
	return nil
}

func (t *Transformer) transformDelete(node *DeleteNode) IOperator {
	return nil
}

func (t *Transformer) transformInsert(node *InsertNode) IOperator {
	return nil
}

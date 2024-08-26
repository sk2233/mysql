/*
@author: sk
@date: 2024/8/25
*/
package main

import (
	"fmt"
	"strings"
)

type Transformer struct {
	Storage *Storage
	Node    INode
}

func NewTransformer(node INode, storage *Storage) *Transformer {
	return &Transformer{Node: node, Storage: storage}
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
	columns := make([]string, 0)
	for _, column := range node.Columns { // 这里不能忽略表名称
		columns = append(columns, fmt.Sprintf("%s.%s", node.Table, column.Value))
	}
	return NewCreateIndexOperator(t.Storage, node.Index, node.Table, columns)
}

func (t *Transformer) transformCreateTable(node *CreateTableNode) IOperator {
	columns := make([]*Column, 0)
	for _, column := range node.Columns {
		var typ int8
		var l int64
		switch column.Type {
		case INT:
			typ = TypInt
			l = 8
		case FLOAT:
			typ = TypFloat
			l = 8
		case VARCHAR:
			typ = TypStr
			l = column.Len // 只有这里信任用户的输入
		case TEXT:
			typ = TypTxt
			l = 8
		default:
			panic(fmt.Sprintf("unknown column type: %s", column.Type))
		}
		if l <= 0 {
			panic(fmt.Sprintf("invalid column %v len %d", column, l))
		}
		columns = append(columns, &Column{
			Name: fmt.Sprintf("%s.%s", node.Table, column.Name),
			Type: typ,
			Len:  l,
		})
	}
	return NewCreateTableOperator(node.Table, columns)
}

func (t *Transformer) transformSelect(node *SelectNode) IOperator {
	// 逻辑转换，语句重写  带有 count() 的非聚合 sql 需要转换为聚合 sql 重写需要对字段做处理

	// 物理算子转换

	// JOIN 先不走索引
	return nil
}

func (t *Transformer) transformUpdate(node *UpdateNode) IOperator {
	// 更新还有原值覆盖写入，必须使用全表扫描
	input := NewTableScanOperator(t.Storage, node.Table)
	input = NewFilterOperator(input, node.Where)
	return NewUpdateOperator(input, t.Storage, node.Table, node.Sets)
}

func (t *Transformer) transformDelete(node *DeleteNode) IOperator {
	t.tidyNodeField(node, node.Table)
	// 可以看下索引是否满足需求，满足可以走索引
	fields := t.extraNodeField(node.Where)
	fields = DistinctSlice(fields)
	index := t.getMostMatchIndex(node.Table, fields)
	var input IOperator
	if index == nil { // 走全表扫描
		input = NewTableScanOperator(t.Storage, node.Table)
	} else { // 走索引
		input = NewIndexScanOperator(t.Storage, index.Name)
	}
	input = NewFilterOperator(input, node.Where)
	return NewDeleteOperator(input, t.Storage, node.Table)
}

func (t *Transformer) transformInsert(node *InsertNode) IOperator {
	meta := GetTable(node.Table)
	data := make([][]any, 0)
	for _, value := range node.Values {
		row := make([]any, 0)
		for i, column := range meta.Columns {
			row = append(row, ValueToAny(value[i], column.Type))
		}
		data = append(data, row)
	}
	return NewInsertOperator(t.Storage, node.Table, data)
}

// tidyXxx 主要用于处理各种 Node 内部 IDNode 的名称问题
func (t *Transformer) tidyNodeField(node INode, table string) {
	if node == nil {
		return
	}
	switch target := node.(type) {
	case *CreateIndexNode:
		for _, column := range target.Columns {
			t.tidyNodeField(column, table)
		}
	case *CreateTableNode:
		for _, column := range target.Columns {
			t.tidyNodeField(column.Name, table)
		}
	case *DeleteNode:
		t.tidyNodeField(target.Where, table)
	case *UpdateNode:
		for _, set := range target.Sets {
			t.tidyNodeField(set, table)
		}
		t.tidyNodeField(target.Where, table)
	case *SetNode:
		t.tidyNodeField(target.Field, table)
		t.tidyNodeField(target.Value, table)
	case *SelectNode:
		// 注意 StarNode 的处理 StarNode 需要扩展为全字段
		fields := make([]INode, 0)
		for _, field := range target.Fields {
			if _, ok := field.(*StarNode); ok {
				meta := GetTable(table)
				for _, column := range meta.Columns {
					fields = append(fields, &IDNode{Value: column.Name})
				}
			} else {
				fields = append(fields, field)
			}
		} // 正常处理字段
		for _, field := range target.Fields {
			t.tidyNodeField(field, table)
		}
		for _, item := range target.Distinct {
			t.tidyNodeField(item, table)
		} // join 表时相关字段必须携带 表名
		//t.tidyNodeField(target.Join.Condition, table)
		if target.Where != nil {
			t.tidyNodeField(target.Where, table)
		}
		for _, group := range target.Groups {
			t.tidyNodeField(group, table)
		}
		for _, order := range target.Orders {
			t.tidyNodeField(order.Field, table)
		}
	case *ExprNode:
		t.tidyNodeField(target.Left, table)
		t.tidyNodeField(target.Right, table)
	case *FuncNode:
		for _, param := range target.Params {
			t.tidyNodeField(param, table)
		}
	case *IDNode: // 真正干活的
		idx := strings.IndexRune(target.Value, '.')
		if idx < 0 { // 没有表名添加表名称
			target.Value = fmt.Sprintf("%s.%s", table, target.Value)
		}
	}
}

// 可能会重复，需要自行去重
func (t *Transformer) extraNodeField(node INode) []string {
	res := make([]string, 0)
	if node == nil {
		return res
	}

	switch target := node.(type) {
	case *DeleteNode:
		res = append(res, t.extraNodeField(target.Where)...)
	case *UpdateNode:
		for _, set := range target.Sets {
			res = append(res, t.extraNodeField(set)...)
		}
		res = append(res, t.extraNodeField(target.Where)...)
	case *SetNode:
		res = append(res, t.extraNodeField(target.Value)...)
		res = append(res, target.Field.Value)
	case *SelectNode:
		// 正常处理字段 这里不再需要处理 StarNode 了，前面处理过了
		for _, field := range target.Fields {
			res = append(res, t.extraNodeField(field)...)
		}
		for _, item := range target.Distinct {
			res = append(res, item.Value)
		}
		if target.Join != nil {
			res = append(res, t.extraNodeField(target.Join.Condition)...)
		}
		if target.Where != nil {
			res = append(res, t.extraNodeField(target.Where)...)
		}
		for _, group := range target.Groups {
			res = append(res, group.Value)
		}
		for _, order := range target.Orders {
			res = append(res, order.Field.Value)
		}
	case *ExprNode:
		res = append(res, t.extraNodeField(target.Left)...)
		res = append(res, t.extraNodeField(target.Right)...)
	case *FuncNode:
		for _, param := range target.Params {
			res = append(res, t.extraNodeField(param)...)
		}
	case *IDNode: // 真正干活的
		res = append(res, target.Value)
	}
	return res
}

func (t *Transformer) getMostMatchIndex(table string, fields []string) *Index {
	idxes := ListIndexes(table)
	var res *Index
	for _, idx := range idxes {
		if len(SubSlice(fields, idx.Columns)) == 0 {
			// 在匹配的索引中找最小的索引
			if res == nil || len(idx.Columns) < len(res.Columns) {
				res = idx
			}
		}
	}
	return res
}

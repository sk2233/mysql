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
			Name: fmt.Sprintf("%s.%s", node.Table, column.Name.Value),
			Type: typ,
			Len:  l,
		})
	}
	return NewCreateTableOperator(node.Table, columns)
}

func (t *Transformer) transformSelect(node *SelectNode) IOperator {
	// 先进行 sql 重写
	// 1. Fields.FuncNode 若是存在聚合函数且其没有 group 需要添加 group 修改语句为聚合函数
	hasAggregate := false
	for _, field := range node.Fields {
		if func0, ok := field.(*FuncNode); ok {
			item := GetFunc(func0.FuncName)
			if item.IsAggregate {
				hasAggregate = true
				break
			}
		}
	}
	if hasAggregate && node.Groups == nil {
		node.Groups = make([]*IDNode, 0) // 这里注意 nil 与 make([]*IDNode, 0) 是不一样的
	}
	// 2. Fields.ImmNode 若是存在全部收集构建扩展表  最后再添加扩展列
	immColumns := make([]*Column, 0)
	immData := make([]any, 0)
	fields := make([]INode, 0)
	columnIdx := 0
	for _, field := range node.Fields {
		if immNode, ok := field.(*ImmNode); ok {
			typ := TokenTypeToType(immNode.Type)
			immColumns = append(immColumns, &Column{
				Name: fmt.Sprintf("Column%d", columnIdx),
				Type: typ,
				Len:  8, // 这里都先认为是 8 位 这个数据仅展示不落表
			})
			immData = append(immData, ValueToAny(&Value{Value: immNode.Value}, typ))
			columnIdx++
		} else {
			fields = append(fields, field)
		}
	}
	node.Fields = fields
	// 扩展处理 *
	hasStar := false
	fields = make([]INode, 0)
	for _, field := range node.Fields {
		if _, ok := field.(*StarNode); ok {
			hasStar = true
		} else { // 移除所有 * 节点
			fields = append(fields, field)
		}
	}
	node.Fields = fields
	if hasStar { // 添加所有 相关字段 节点
		table := GetTable(node.From)
		for _, column := range table.Columns {
			node.Fields = append(node.Fields, &IDNode{Value: column.Name})
		}
		if node.Join != nil {
			table = GetTable(node.Join.Table)
			for _, column := range table.Columns {
				node.Fields = append(node.Fields, &IDNode{Value: column.Name})
			}
		}
	}
	// 整理节点并移除重复 IDNode 节点
	if node.Join == nil { // 只有非 join 情况下可以省略表名称
		t.tidyNodeField(node, node.From)
	}
	idNodeSet := make(map[string]struct{})
	fields = make([]INode, 0)
	for _, field := range node.Fields {
		if idNode, ok := field.(*IDNode); ok {
			if _, has := idNodeSet[idNode.Value]; !has {
				idNodeSet[idNode.Value] = struct{}{}
				fields = append(fields, field)
			}
		} else { // 这里不是 IDNode 就是 FuncNode
			fields = append(fields, field)
		}
	}
	node.Fields = fields
	// 先处理表与 join 再处理 where 条件   只有索引覆盖才走索引
	fieldNames := t.extraNodeField(node)
	fieldNames = DistinctSlice(fieldNames) // 先处理 from
	fromTableFields := t.getTableFields(node.From, fieldNames)
	fromIndex := t.getMostMatchIndex(node.From, fromTableFields)
	var input IOperator
	if fromIndex != nil {
		input = NewIndexScanOperator(t.Storage, fromIndex.Name)
	} else {
		input = NewTableScanOperator(t.Storage, node.From)
	} // 处理 join
	if node.Join != nil {
		joinTableFields := t.getTableFields(node.Join.Table, fieldNames)
		joinIndex := t.getMostMatchIndex(node.Join.Table, joinTableFields)
		if joinIndex != nil {
			input = NewJoinOperator(input, NewIndexScanOperator(t.Storage, joinIndex.Name), node.Join.Condition)
		} else {
			input = NewJoinOperator(input, NewTableScanOperator(t.Storage, node.Join.Table), node.Join.Condition)
		}
	}
	if node.Where != nil {
		input = NewFilterOperator(input, node.Where)
	}
	// 再处理 group distinct
	if node.Groups != nil {
		groupColumns := make([]string, 0)
		for _, column := range node.Groups {
			groupColumns = append(groupColumns, column.Value)
		}
		funcs0 := make([]*FuncNode, 0)
		for _, field := range node.Fields {
			if func0, ok := field.(*FuncNode); ok {
				funcs0 = append(funcs0, func0)
			}
		}
		input = NewGroupOperator(input, groupColumns, funcs0)
	}
	if node.Distinct != nil {
		distinctFields := make([]string, 0)
		for _, idNode := range node.Distinct {
			distinctFields = append(distinctFields, idNode.Value)
		}
		input = NewDistinctOperator(input, distinctFields)
	}
	// 最后再做 order by   limit
	if node.Orders != nil {
		input = NewSortOperator(input, node.Orders)
	}
	if node.Limit != nil {
		input = NewLimitOperator(input, node.Limit.Limit, node.Limit.Offset)
	}
	// 处理非聚合函数
	funcs0 := make([]*FuncNode, 0)
	for _, field := range node.Fields {
		if func0, ok := field.(*FuncNode); ok {
			funcs0 = append(funcs0, func0)
		}
	}
	if len(funcs0) > 0 { // 内部会再次过滤掉聚合函数
		input = NewFuncExecOperator(input, funcs0)
	}
	// 选择字段裁剪 添加扩展列(扩展列没有按原始顺序，会直接排到后面)
	fieldNames = make([]string, 0)
	for _, field := range node.Fields {
		if idNode, ok1 := field.(*IDNode); ok1 {
			fieldNames = append(fieldNames, idNode.Value)
		} else if funcNode, ok2 := field.(*FuncNode); ok2 {
			fieldNames = append(fieldNames, GetFuncColumnName(funcNode))
		} else {
			panic(fmt.Sprintf("not support node %v", node))
		}
	}
	input = NewProjectionOperator(input, fieldNames)
	if len(immColumns) > 0 && len(immData) > 0 {
		input = NewExpandImmOperator(input, immColumns, immData)
	}
	return input
}

func (t *Transformer) transformUpdate(node *UpdateNode) IOperator {
	t.tidyNodeField(node, node.Table)
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
		// 正常处理字段
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

func (t *Transformer) getTableFields(table string, fields []string) []string {
	res := make([]string, 0)
	table = table + "."
	for _, field := range fields {
		if strings.HasPrefix(field, table) {
			res = append(res, field)
		}
	}
	return res
}

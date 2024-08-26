/*
@author: sk
@date: 2024/8/15
*/
package main

import (
	"fmt"
	"sort"
)

// 把各种查询，等操作转换为算子，流式处理  物理执行计划组装用的算子，这里暂时忽略逻辑执行计划

type IOperator interface {
	Open()                 // 初始化
	Close()                // 销毁
	Next() []any           // 获取一行数据 没有数据了返回 nil 应该能在没有数据后持续返回 nil 注意
	Reset()                // 重置状态
	GetColumns() []*Column // 获取该流的输出列
}

type InputOperator struct { // 对于简单单继承通道的默认实现
	Input IOperator
}

func (i *InputOperator) GetColumns() []*Column {
	return i.Input.GetColumns()
}

func (i *InputOperator) Open() {
	i.Input.Open()
}

func (i *InputOperator) Close() {
	i.Input.Close()
}

func (i *InputOperator) Next() []any {
	return i.Input.Next()
}

func (i *InputOperator) Reset() {
	i.Input.Reset()
}

func NewInputOperator(input IOperator) *InputOperator {
	return &InputOperator{Input: input}
}

type OnceOperator struct {
	Columns []*Column
	Used    bool
	Action  func() int64 // 子类必须赋值
}

func NewOnceOperator(action func() int64) *OnceOperator {
	return &OnceOperator{Action: action}
}

func (o *OnceOperator) Open() {
	o.Columns = []*Column{{
		Name: "effected_row",
		Type: TypInt,
		Len:  8,
	}}
}

func (o *OnceOperator) Close() {
}

func (o *OnceOperator) Next() []any {
	if o.Used {
		return nil
	}
	o.Used = true
	return []any{o.Action()}
}

func (o *OnceOperator) Reset() {
	panic("not support reset")
}

func (o *OnceOperator) GetColumns() []*Column {
	return o.Columns
}

//======================TableScanOperator=========================

// 只做简单的全表扫描，不包含条件下推
type TableScanOperator struct { // 只有输出，没有输入的流
	Storage *Storage
	Table   string
	Offset  int64
	Columns []*Column
}

func (t *TableScanOperator) GetColumns() []*Column {
	return t.Columns
}

func (t *TableScanOperator) Reset() {
	t.Offset = 0
}

func (t *TableScanOperator) Open() {
	table := GetTable(t.Table)
	t.Columns = append(table.Columns, &Column{
		Name: "offset",
		Type: TypInt,
		Len:  8,
	})
}

func (t *TableScanOperator) Close() {
}

func (t *TableScanOperator) Next() []any {
	var res []any
	var currOffset int64
	res, currOffset, t.Offset = t.Storage.NextData(t.Table, t.Offset)
	return append(res, currOffset) // 最后一个就是 offset
}

func NewTableScanOperator(storage *Storage, table string) IOperator {
	return &TableScanOperator{Storage: storage, Table: table, Offset: 0}
}

//=====================IndexScanOperator============================

type IndexScanOperator struct {
	Storage *Storage
	Index   string
	Node    *BTreeNode
	NodeIdx int
	Columns []*Column
}

func (i *IndexScanOperator) GetColumns() []*Column {
	return i.Columns
}

func (i *IndexScanOperator) Reset() {
	i.Node = nil
	i.NodeIdx = 0
}

func (i *IndexScanOperator) Open() {
	index := GetIndex(i.Index)
	table := GetTable(index.TableName)
	i.Columns = PickColumn(index.Columns, table.Columns)
	i.Columns = append(i.Columns, &Column{ // 需要额外添加索引列
		Name: "data",
		Type: TypInt,
		Len:  8,
	})
}

func (i *IndexScanOperator) Close() {
}

func (i *IndexScanOperator) Next() []any {
	var res []any
	res, i.Node, i.NodeIdx = i.Storage.NextIndex(i.Index, i.Node, i.NodeIdx)
	return res
}

func NewIndexScanOperator(storage *Storage, index string) IOperator {
	return &IndexScanOperator{Storage: storage, Index: index, Node: nil, NodeIdx: 0}
}

//=======================JoinOperator===========================

// 有双层循环连接 与 hash连接(需要先构建hash表且需要落盘) 这里使用双层循环连接
type JoinOperator struct { // 只支持 内连接 (笛卡尔积就是没有条件的内链接)
	Left, Right IOperator // 只支持两个
	LeftData    []any
	Columns     []*Column
	Expr        *ExprNode // 连接条件
}

func (j *JoinOperator) GetColumns() []*Column {
	return j.Columns
}

func (j *JoinOperator) Reset() {
	j.Left.Reset()
	j.Right.Reset()
	j.LeftData = j.Left.Next()
}

func (j *JoinOperator) Open() {
	j.Left.Open() // 子表全部打开
	j.Right.Open()
	j.LeftData = j.Left.Next()
	j.Columns = append(j.Left.GetColumns(), j.Right.GetColumns()...)
}

func (j *JoinOperator) Close() {
	j.Left.Close() // 子表全部打开
	j.Right.Close()
}

func (j *JoinOperator) Next() []any {
	for {
		if j.LeftData == nil {
			return nil
		}
		rightData := j.Right.Next()
		if rightData == nil {
			j.LeftData = j.Left.Next()
			j.Right.Reset()
			continue
		}
		res := append(j.LeftData, rightData...)
		if CalculateExpr(j.Expr, j.Columns, res) {
			return res
		}
	}
}

func NewJoinOperator(left IOperator, right IOperator, expr *ExprNode) IOperator {
	return &JoinOperator{Left: left, Right: right, Expr: expr}
}

//=========================ProjectionOperator==============================

type ProjectionOperator struct {
	*InputOperator
	SelectFields []string
	Columns      []*Column
	DataIdx      []int
}

func (p *ProjectionOperator) GetColumns() []*Column {
	return p.Columns
}

func (p *ProjectionOperator) Open() {
	p.InputOperator.Open()
	columns := p.Input.GetColumns()
	columnMap := make(map[string]*Column)
	idxMap := make(map[string]int)
	for i, column := range columns {
		columnMap[column.Name] = column
		idxMap[column.Name] = i
	}
	for _, field := range p.SelectFields {
		if column, ok := columnMap[field]; ok {
			p.Columns = append(p.Columns, column)
			p.DataIdx = append(p.DataIdx, idxMap[field])
		} else {
			panic(fmt.Sprintf("column %s not found", field))
		}
	}
}

func (p *ProjectionOperator) Next() []any {
	temp := p.Input.Next()
	if temp == nil {
		return nil
	}
	res := make([]any, 0)
	for _, idx := range p.DataIdx {
		res = append(res, temp[idx])
	}
	return res
}

func NewProjectionOperator(input IOperator, selectFields []string) IOperator {
	return &ProjectionOperator{InputOperator: NewInputOperator(input), SelectFields: selectFields}
}

//======================DistinctOperator=====================

type DistinctOperator struct { // 对某列去重返回(允许多列去重)
	*InputOperator
	DistinctFields []string // 至少有一个
	Set            map[string]struct{}
	DataIdx        []int
	Columns        []*Column
}

func (d *DistinctOperator) GetColumns() []*Column {
	return d.Columns
}

func (d *DistinctOperator) Open() {
	d.Input.Open()
	columns := d.Input.GetColumns()
	idxMap := make(map[string]int)
	columnMap := make(map[string]*Column)
	for i, column := range columns {
		idxMap[column.Name] = i
		columnMap[column.Name] = column
	}
	for _, field := range d.DistinctFields {
		if idx, ok := idxMap[field]; ok {
			d.DataIdx = append(d.DataIdx, idx)
			d.Columns = append(d.Columns, columnMap[field])
		} else {
			panic(fmt.Sprintf("column %s not found", field))
		}
	}
}

func (d *DistinctOperator) Reset() {
	d.InputOperator.Reset()
	d.Set = make(map[string]struct{})
}

func (d *DistinctOperator) Next() []any {
	for {
		temp := d.Input.Next()
		if temp == nil {
			return nil
		}
		res := make([]any, 0)
		key := fmt.Sprintf("%v", temp[d.DataIdx[0]])
		res = append(res, temp[d.DataIdx[0]])
		for i := 1; i < len(d.DataIdx); i++ {
			key = fmt.Sprintf("%v#%v", key, temp[d.DataIdx[i]])
			res = append(res, temp[d.DataIdx[i]])
		}
		if _, ok := d.Set[key]; !ok {
			d.Set[key] = struct{}{}
			return res
		}
	}
}

func NewDistinctOperator(input IOperator, distinctFields []string) IOperator {
	return &DistinctOperator{InputOperator: NewInputOperator(input), DistinctFields: distinctFields, Set: make(map[string]struct{})}
}

//======================FilterOperator========================

type FilterOperator struct {
	*InputOperator
	Expr *ExprNode // 判断条件
}

func (f *FilterOperator) Next() []any {
	for {
		res := f.Input.Next()
		if res == nil {
			return nil
		}
		if CalculateExpr(f.Expr, f.Input.GetColumns(), res) {
			return res
		}
	}
}

func NewFilterOperator(input IOperator, expr *ExprNode) IOperator {
	return &FilterOperator{InputOperator: NewInputOperator(input), Expr: expr}
}

//============================GroupOperator=============================

// 所有列都是 表名.列名 的形式 sql可以简写，会自动扩充 起别名还是会最终替换为表名称 * 也是这里替换的
// 常量列实际就是与一个只有一行数据的临时表 join 笛卡尔积  会改变列的输出
type GroupOperator struct { // 支持多列分组
	*InputOperator
	GroupColumns []string // 聚合的列  不能简单使用列名称，多表可能存在重复列名 可以统一规划为 表名.列名 长度可以为空
	// SUM AVG MAX MIN COUNT 有 count 且不是聚合查询的语句需要进行改写为聚合语句 GroupColumns 可以为空
	Funcs []*FuncNode // 聚合函数操作的其他列 例如 max(height)  group by age 聚合函数只能有一个入参且必须为 IDNode
	// 需要全放到内存 暂时不考虑落盘方案
	Data    [][]any
	DataIdx int
	Columns []*Column
}

func (g *GroupOperator) GetColumns() []*Column {
	return g.Columns
}

func (g *GroupOperator) GetFuncIDNode(func0 *FuncNode) *IDNode {
	if len(func0.Params) != 1 {
		panic(fmt.Sprintf("func0 must one parameter"))
	}
	return func0.Params[0].(*IDNode)
}

func (g *GroupOperator) Open() {
	g.InputOperator.Open()
	dataIdx := make([]int, 0)
	funcIdx := make([]int, 0)
	// 先组装列信息
	columns := g.Input.GetColumns()
	columnMap := make(map[string]*Column)
	idxMap := make(map[string]int)
	for i, column := range columns {
		columnMap[column.Name] = column
		idxMap[column.Name] = i
	}
	for _, field := range g.GroupColumns {
		if column, ok := columnMap[field]; ok {
			g.Columns = append(g.Columns, column)
			dataIdx = append(dataIdx, idxMap[field])
		} else {
			panic(fmt.Sprintf("column %s not found", field))
		}
	}
	for _, item := range g.Funcs {
		node := g.GetFuncIDNode(item)
		if column, ok := columnMap[node.Value]; ok {
			func0 := GetFunc(item.FuncName)
			typ, l := func0.RetType(column) // 获取对应类型与长度
			g.Columns = append(g.Columns, &Column{
				Name: fmt.Sprintf("%v#%v", node.Value, item.FuncName), // 列名需要拼接函数名
				Type: typ,
				Len:  l,
			})
			funcIdx = append(funcIdx, idxMap[node.Value])
		} else {
			panic(fmt.Sprintf("column %s not found", node.Value))
		}
	}
	// 拉取信息进行组装
	temp := make(map[string][][]any)
	for {
		res := g.Input.Next()
		if res == nil {
			break
		}
		key := g.GenKey(res, dataIdx)
		temp[key] = append(temp[key], res)
	}
	for _, items := range temp {
		res := make([]any, 0) // 组装分组数据
		for _, idx := range dataIdx {
			res = append(res, items[0][idx]) // 分组字段都是一样的随便选一个就行 这里直接用第一个的
		} // 组装函数数据
		for i, item := range g.Funcs {
			func0 := GetFunc(item.FuncName)
			params := make([]*Value, 0)
			column := columns[funcIdx[i]]
			for _, data := range items {
				params = append(params, &Value{
					Type: column.Type,
					Data: data[funcIdx[i]],
				})
			}
			res = append(res, func0.Call(params))
		}
		g.Data = append(g.Data, res)
	}
	g.DataIdx = 0
}

func (g *GroupOperator) GenKey(res []any, idx []int) string {
	if len(idx) == 0 {
		return ""
	}
	key := fmt.Sprintf("%v", res[idx[0]])
	for i := 1; i < len(idx); i++ {
		key = fmt.Sprintf("%v#%v", key, res[idx[i]])
	}
	return key
}

func (g *GroupOperator) Reset() {
	g.InputOperator.Reset()
	g.DataIdx = 0
}

func (g *GroupOperator) Next() []any {
	if g.DataIdx < len(g.Data) {
		g.DataIdx++
		return g.Data[g.DataIdx-1]
	} else {
		return nil
	}
}

func NewGroupOperator(input IOperator, groupColumns []string, funcs []*FuncNode) IOperator {
	return &GroupOperator{InputOperator: NewInputOperator(input), GroupColumns: groupColumns, Funcs: funcs}
}

//=======================SortOperator==========================

// 排序分为 内排序 外排序(数据太大内存放不下)
// 外排序，排序完放在临时文件中，每次 next读取一部分进行返回
// 暂时仅实现内排序
type SortOperator struct {
	*InputOperator
	Orders  []*OrderNode // 按顺序来，支持多列排序
	Data    [][]any
	DataIdx int
}

func (s *SortOperator) Open() {
	s.InputOperator.Open()
	// 准备数据
	for {
		res := s.Input.Next()
		if res == nil {
			break
		}
		s.Data = append(s.Data, res)
	}
	columns := s.Input.GetColumns()
	columnMap := make(map[string]*Column)
	idxMap := make(map[string]int)
	for idx, column := range columns {
		idxMap[column.Name] = idx
		columnMap[column.Name] = column
	}
	dataIdx := make([]int, 0)
	dataColumns := make([]*Column, 0)
	for _, order := range s.Orders {
		if idx, ok := idxMap[order.Field.Value]; ok {
			dataIdx = append(dataIdx, idx)
			dataColumns = append(dataColumns, columnMap[order.Field.Value])
		} else {
			panic(fmt.Sprintf("field %s not found", order.Field.Value))
		}
	}
	sort.Slice(s.Data, func(i, j int) bool {
		for k, order := range s.Orders {
			column := dataColumns[k]
			res := CompareValue(&Value{
				Type: column.Type,
				Data: s.Data[i][dataIdx[k]],
			}, &Value{
				Type: column.Type,
				Data: s.Data[j][dataIdx[k]],
			})
			if res == 0 { // 当前比较一致进行下一级
				continue
			}
			if order.Desc {
				return res > 0
			} else {
				return res < 0
			}
		}
		return true
	})
	s.DataIdx = 0
}

func (s *SortOperator) Reset() {
	s.InputOperator.Reset()
	s.DataIdx = 0
}

func (s *SortOperator) Next() []any {
	if s.DataIdx < len(s.Data) {
		s.DataIdx++
		return s.Data[s.DataIdx-1]
	} else {
		return nil
	}
}

func NewSortOperator(input IOperator, orders []*OrderNode) IOperator {
	return &SortOperator{InputOperator: NewInputOperator(input), Orders: orders}
}

//==========================LimitOperator=======================

type LimitOperator struct { // 只能调用指定数目个 可以把数目往下传递，方便后续节点优化  offset 也支持
	*InputOperator
	Limit  int
	Offset int
	Count  int
}

func (l *LimitOperator) Next() []any {
	if l.Count < l.Limit {
		l.Count++
		return l.Input.Next()
	} else {
		return nil
	}
}

func (l *LimitOperator) Open() {
	l.InputOperator.Open()
	for i := 0; i < l.Offset; i++ {
		if res := l.Input.Next(); res == nil {
			break
		}
	}
}

func (l *LimitOperator) Reset() {
	l.InputOperator.Reset()
	for i := 0; i < l.Offset; i++ {
		if res := l.Input.Next(); res == nil {
			break
		}
	}
	l.Count = 0
}

func NewLimitOperator(input IOperator, limit int, offset int) IOperator {
	return &LimitOperator{Limit: limit, Offset: offset, InputOperator: NewInputOperator(input), Count: 0}
}

//=====================InsertOperator====================

type InsertOperator struct {
	*OnceOperator
	Table   string
	Data    [][]any // 可以设置多条数据  若需要支持 select insert 这里也需要使用 IOperator 作为输入
	Storage *Storage
}

func (i *InsertOperator) InsertData() int64 {
	for _, data := range i.Data {
		i.Storage.InsertData(i.Table, data)
	}
	return int64(len(i.Data))
}

func NewInsertOperator(storage *Storage, table string, data [][]any) IOperator {
	res := &InsertOperator{Table: table, Data: data, Storage: storage}
	res.OnceOperator = NewOnceOperator(res.InsertData)
	return res
}

//======================UpdateOperator=============================

type UpdateOperator struct { // 涉及一个选择 需要根据条件获取对应记录的偏移 再做更改，同时更新索引
	*InputOperator // 基本就是指定为全表扫描了，既需要全表的数据，也需要偏移
	Columns        []*Column
	Table          string
	Storage        *Storage
	Sets           []*SetNode
	Used           bool
}

func (u *UpdateOperator) Next() []any {
	if u.Used { // 防止多次执行
		return nil
	}
	u.Used = true
	columns := u.Input.GetColumns()
	idxMap := make(map[string]int)
	columnMap := make(map[string]*Column)
	for idx, column := range columns {
		idxMap[column.Name] = idx
		columnMap[column.Name] = column
	}
	setIdx := make([]int, 0)
	setColumns := make([]*Column, 0)
	for _, set := range u.Sets {
		if idx, ok := idxMap[set.Field.Value]; ok {
			setIdx = append(setIdx, idx)
			setColumns = append(setColumns, columnMap[set.Field.Value])
		} else {
			panic(fmt.Sprintf("field %s not found", set.Field.Value))
		}
	}
	effectedRow := int64(0)
	for {
		res := u.Input.Next()
		if res == nil {
			break
		}
		for i, set := range u.Sets { // 尽量 set 值不要依赖其他有本次修改的值，可能出现不可控情况
			column := setColumns[i]
			val := ParseValue(set.Value, columns, res)
			res[setIdx[i]] = ValueToAny(val, column.Type)
		}
		offsetIdx := len(res) - 1
		offset := res[offsetIdx].(int64) // 最后一个就是 offset
		u.Storage.UpdateData(u.Table, offset, res[:offsetIdx])
		effectedRow++
	}
	return []any{effectedRow}
}

func (u *UpdateOperator) Reset() {
	panic("not support reset")
}

func (u *UpdateOperator) GetColumns() []*Column {
	return u.Columns
}

func (u *UpdateOperator) Open() {
	u.InputOperator.Open()
	u.Columns = []*Column{{
		Name: "effected_row",
		Type: TypInt,
		Len:  8,
	}}
}

func NewUpdateOperator(input IOperator, storage *Storage, table string, sets []*SetNode) IOperator {
	return &UpdateOperator{InputOperator: NewInputOperator(input), Table: table, Storage: storage, Sets: sets, Used: false}
}

//==========================DeleteOperator=============================

type DeleteOperator struct { // 涉及一个选择 需要根据条件获取对应记录的偏移 再做删除，同时删除索引
	*InputOperator
	Storage *Storage
	Table   string
	Columns []*Column
	Used    bool
}

func (d *DeleteOperator) Next() []any {
	if d.Used {
		return nil
	}
	d.Used = true
	effectedRow := int64(0)
	for {
		res := d.Input.Next()
		if res == nil {
			break
		}
		offset := res[len(res)-1].(int64)
		d.Storage.DeleteData(d.Table, offset)
		effectedRow++
	}
	return []any{effectedRow}
}

func (d *DeleteOperator) Reset() {
	panic("not support reset")
}

func (d *DeleteOperator) GetColumns() []*Column {
	return d.Columns
}

func (d *DeleteOperator) Open() {
	d.InputOperator.Open()
	d.Columns = []*Column{{
		Name: "effected_row",
		Type: TypInt,
		Len:  8,
	}}
}

func (d *DeleteOperator) Close() {
	d.InputOperator.Close()
}

func NewDeleteOperator(input IOperator, storage *Storage, table string) IOperator {
	return &DeleteOperator{InputOperator: NewInputOperator(input), Storage: storage, Table: table, Used: false}
}

//====================CreateTableOperator=======================

type CreateTableOperator struct { // 暂时不支持表结构的修改
	*OnceOperator
	Table   string
	Columns []*Column
}

func (c *CreateTableOperator) CreateTable() int64 {
	table := &Table{
		Name:    c.Table,
		Columns: c.Columns,
	}
	AddTable(table)
	return 1
}

func NewCreateTableOperator(table string, columns []*Column) IOperator {
	res := &CreateTableOperator{Table: table, Columns: columns}
	res.OnceOperator = NewOnceOperator(res.CreateTable)
	return res
}

//========================CreateIndexOperator=========================

type CreateIndexOperator struct { // 暂时不支持索引结构的修改
	*OnceOperator
	Storage *Storage
	Index   string
	Table   string
	Columns []string
}

func (c *CreateIndexOperator) CreateIndex() int64 {
	// 添加索引
	index := &Index{
		Name:      c.Index,
		TableName: c.Table,
		Columns:   c.Columns,
	}
	AddIndex(index)
	// 为存量数据创建索引
	operator := NewTableScanOperator(c.Storage, c.Table)
	operator.Open()
	effectedRow := int64(0)
	btree := c.Storage.OpenIndex(index.Name)
	for {
		res := operator.Next()
		if res == nil {
			break
		}
		data0 := PickData(index.Columns, operator.GetColumns(), res)
		offset := res[len(res)-1].(int64)
		btree.AddData(data0, offset)
		effectedRow++
	}
	operator.Close()
	return effectedRow
}

func NewCreateIndexOperator(storage *Storage, index string, table string, columns []string) IOperator {
	res := &CreateIndexOperator{Index: index, Table: table, Columns: columns, Storage: storage}
	res.OnceOperator = NewOnceOperator(res.CreateIndex)
	return res
}

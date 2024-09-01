/*
@author: sk
@date: 2024/8/10
*/
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
)

// 元数据依旧是一张表，不过其元数据是写死的，且其数据会在系统启动时加载进内存 先直接使用 json 实现

type Column struct {
	Name string
	Type int8
	Len  int64
}

func (c *Column) String() string {
	return fmt.Sprintf("%s(%d %d)", c.Name, c.Type, c.Len)
}

type Table struct {
	Name    string
	Columns []*Column
}

type Index struct {
	Name      string
	TableName string
	Columns   []string // 这里可以直接使用名称引用表中的列
}

// 函数元数据还需要定义输入与输出
type Func struct { // 函数定义
	Name             string
	IsAggregate      bool                               // 是否为聚合函数
	RetType          func() (int8, int64)               // 非聚合函数，返回值类型与长度是固定的
	AggregateRetType func(column *Column) (int8, int64) // 聚合函数需要根据对应列决定返回类型与长度
	Call             func(params []*Value) any          // 计算最终值
}

var (
	tables  = make([]*Table, 0)
	indexes = make([]*Index, 0)
	// 待实现的聚合函数 count sum avg max min
	funcs = []*Func{{ // 函数是内置的不需要序列化
		Name:        "MAX",
		IsAggregate: true,
		AggregateRetType: func(column *Column) (int8, int64) {
			return column.Type, column.Len
		},
		Call: func(params []*Value) any { // 不会为 0 个
			for i := 1; i < len(params); i++ {
				if CompareValue(params[i], params[0]) > 0 {
					params[0] = params[i]
				}
			}
			return params[0].Data
		},
	}, {
		Name:        "COUNT",
		IsAggregate: true,
		AggregateRetType: func(column *Column) (int8, int64) {
			return TypInt, 8
		},
		Call: func(params []*Value) any {
			return int64(len(params))
		},
	}, {
		Name:        "TEST",
		IsAggregate: false,
		RetType: func() (int8, int64) {
			return TypInt, 8
		},
		Call: func(params []*Value) any {
			return params[0].ToInt() * 2
		},
	}}
)

// 元数据信息先以 json 形式存储，因为经常使用需要常驻内存

func LoadCatalog() {
	bs, err := os.ReadFile(path.Join(BasePath, CatalogTable))
	HandleErr(err)
	HandleErr(json.Unmarshal(bs, &tables))

	bs, err = os.ReadFile(path.Join(BasePath, CatalogIndex))
	HandleErr(err)
	HandleErr(json.Unmarshal(bs, &indexes))
}

func SaveCatalog() {
	bs, err := json.Marshal(tables)
	HandleErr(err)
	HandleErr(os.WriteFile(path.Join(BasePath, CatalogTable), bs, 0666))

	bs, err = json.Marshal(indexes)
	HandleErr(err)
	HandleErr(os.WriteFile(path.Join(BasePath, CatalogIndex), bs, 0666))
}

func GetFunc(name string) *Func {
	name = strings.ToUpper(name)
	for _, func0 := range funcs {
		if func0.Name == name {
			return func0
		}
	}
	panic(fmt.Sprintf("func %s not found", name))
}

func GetTable(table string) *Table {
	for _, item := range tables {
		if item.Name == table {
			return item
		}
	}
	panic("table not found: " + table)
}

func AddTable(table *Table) {
	for _, item := range tables {
		if item.Name == table.Name {
			panic(fmt.Sprintf("table %s already exists", table.Name))
		}
	}
	tables = append(tables, table)
}

func GetIndex(index string) *Index {
	for _, item := range indexes {
		if item.Name == index {
			return item
		}
	}
	panic("index not found: " + index)
}

func AddIndex(index *Index) {
	for _, item := range indexes {
		if item.Name == index.Name {
			panic(fmt.Sprintf("index %s already exists", index.Name))
		}
	}
	indexes = append(indexes, index)
}

func ListIndexes(table string) []*Index {
	idxes := make([]*Index, 0)
	for _, index := range indexes {
		if index.TableName == table {
			idxes = append(idxes, index)
		}
	}
	return idxes
}

/*
@author: sk
@date: 2024/8/6
*/
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

func IsDigit(val byte) bool {
	return val >= '0' && val <= '9'
}

func IsAlpha(val byte) bool {
	if val >= 'a' && val <= 'z' {
		return true
	}
	if val >= 'A' && val <= 'Z' {
		return true
	}
	return val == '_'
}

func OpenOrCreate(path string) *os.File {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		file, err = os.Create(path)
		HandleErr(err)
		return file
	}
	HandleErr(err)
	return file
}

func HandleErr(err error) {
	if err != nil {
		panic(err)
	}
}

func Int64ToByte(val int64) []byte {
	return Uint64ToByte(uint64(val))
}

func ByteToInt64(bs []byte) int64 {
	u64 := ByteToUint64(bs)
	return int64(u64)
}

func Float64ToByte(val float64) []byte {
	return Uint64ToByte(math.Float64bits(val))
}

func ByteToFloat64(bs []byte) float64 {
	return math.Float64frombits(ByteToUint64(bs))
}

func StrToByte(val string, l int64) []byte {
	res := make([]byte, l)
	copy(res, val)
	return res
}

func ByteToStr(bs []byte) string {
	return strings.TrimRight(string(bs), "\x00")
}

func Uint64ToByte(val uint64) []byte {
	res := make([]byte, 8)
	binary.LittleEndian.PutUint64(res, val)
	return res
}

func ByteToUint64(bs []byte) uint64 {
	return binary.LittleEndian.Uint64(bs)
}

func PickColumn(selectColumns []string, columns []*Column) []*Column {
	columnMap := make(map[string]*Column)
	for _, column := range columns {
		columnMap[column.Name] = column
	}
	columnRes := make([]*Column, 0)
	for _, column := range selectColumns {
		columnRes = append(columnRes, columnMap[column])
	}
	return columnRes
}

func PickData(selectColumns []string, columns []*Column, data []any) []any {
	dataMap := make(map[string]any)
	for i, column := range columns {
		dataMap[column.Name] = data[i]
	}
	dataRes := make([]any, 0)
	for _, column := range selectColumns {
		dataRes = append(dataRes, dataMap[column])
	}
	return dataRes
}

func GetColumnSize(columns []*Column) int {
	res := 0
	for _, column := range columns {
		res += int(column.Len)
	}
	return res
}

func ColumnBatchCompare(vals1 []any, vals2 []any, columns []*Column) int {
	for i := 0; i < len(columns); i++ {
		res := ColumnCompare(vals1[i], vals2[i], columns[i])
		if res != 0 {
			return res
		}
	}
	return 0
}

func Compare[T int64 | float64 | string](val1, val2 T) int {
	if val1 > val2 {
		return 1
	} else if val1 < val2 {
		return -1
	} else {
		return 0
	}
}

func ColumnCompare(val1 any, val2 any, column *Column) int {
	switch column.Type {
	case TypInt:
		return Compare(val1.(int64), val2.(int64))
	case TypFloat:
		return Compare(val1.(float64), val2.(float64))
	case TypStr, TypTxt: // 索引是不支持 不定长文本的，这里先不做区分
		return Compare(val1.(string), val2.(string))
	default:
		panic(fmt.Errorf("unknown column type: %v", column.Type))
	}
}

type TxtReader func(offset int64) string

func BatchByte2Data(bs []byte, columns []*Column, reader TxtReader) []any {
	data := make([]any, 0)
	i := 0
	for _, column := range columns {
		data = append(data, Byte2Data(bs[i:i+int(column.Len)], column, reader))
		i += int(column.Len)
	}
	return data
}

func Byte2Data(bs []byte, column *Column, reader TxtReader) any {
	switch column.Type {
	case TypInt:
		return ByteToInt64(bs)
	case TypFloat:
		return ByteToFloat64(bs)
	case TypStr:
		return ByteToStr(bs)
	case TypTxt:
		offset := ByteToInt64(bs)
		return reader(offset)
	default:
		panic(fmt.Sprintf("unknown column type: %v", column.Type))
	}
}

type TxtWriter func(value string) int64

func Data2Byte(data any, column *Column, writer TxtWriter) []byte {
	switch column.Type {
	case TypInt:
		return Int64ToByte(data.(int64))
	case TypFloat:
		return Float64ToByte(data.(float64))
	case TypStr:
		return StrToByte(data.(string), column.Len)
	case TypTxt:
		offset := writer(data.(string))
		return Int64ToByte(offset)
	default:
		panic(fmt.Sprintf("unknown column type: %v", column.Type))
	}
}

func BatchData2Byte(data []any, columns []*Column, writer TxtWriter) []byte {
	res := &bytes.Buffer{}
	for i, column := range columns {
		res.Write(Data2Byte(data[i], column, writer))
	}
	return res.Bytes()
}

func CloneSlice[T any](arr []T) []T {
	res := make([]T, 0)
	for _, value := range arr {
		res = append(res, value)
	}
	return res
}

type Value struct {
	Type  int8 // 类型确定的话，Data 就是对应的类型 否则从Value 进行自适应转换
	Data  any
	Value string
}

func (v *Value) ToInt() int64 {
	if v.Type == 0 {
		res, err := strconv.ParseInt(v.Value, 10, 64)
		HandleErr(err)
		return res
	}
	if v.Type != TypInt {
		panic(fmt.Sprintf("type %v not int", v.Type))
	}
	return v.Data.(int64)
}

func (v *Value) ToFloat() float64 {
	if v.Type == 0 {
		res, err := strconv.ParseFloat(v.Value, 64)
		HandleErr(err)
		return res
	}
	if v.Type != TypFloat {
		panic(fmt.Sprintf("type %v not float", v.Type))
	}
	return v.Data.(float64)
}

func (v *Value) ToStr() string {
	if v.Type == 0 {
		return v.Value
	}
	if v.Type != TypStr {
		panic(fmt.Sprintf("type %v not str", v.Type))
	}
	return v.Data.(string)
}

func (v *Value) ToBool() bool { // bool值暂时不接受字面量
	if v.Type != TypBool {
		panic(fmt.Sprintf("type %v not bool", v.Type))
	}
	return v.Data.(bool)
}

func ValueToAny(value *Value, typ int8) any {
	switch typ {
	case TypInt:
		return value.ToInt()
	case TypFloat:
		return value.ToFloat()
	case TypStr, TypTxt:
		return value.ToStr()
	case TypBool:
		return value.ToBool()
	default:
		panic(fmt.Sprintf("unknown column type: %v", typ))
	}
}

func ParseValue(node INode, columns []*Column, data []any) *Value {
	switch temp := node.(type) {
	case *IDNode:
		for i, column := range columns {
			if temp.Value == column.Name {
				return &Value{
					Type: column.Type,
					Data: data[i],
				}
			}
		}
		panic(fmt.Sprintf("column %v not found", temp.Value))
	case *ImmNode:
		return &Value{
			Value: temp.Value,
		}
	case *FuncNode:
		func0 := GetFunc(temp.FuncName)
		params := make([]*Value, 0)
		for _, param := range temp.Params {
			params = append(params, ParseValue(param, columns, data))
		}
		typ, _ := func0.RetType()
		val := func0.Call(params) // 这里 typ 若是文本必须使用 TypStr 不要使用 TypTxt
		return &Value{
			Type: typ,
			Data: val,
		}
	case *ExprNode:
		return &Value{
			Type: TypBool,
			Data: CalculateExpr(temp, columns, data),
		}
	default:
		panic(fmt.Sprintf("not support node %v", node))
	}
}

func CompareValue(val1 *Value, val2 *Value) int {
	typ := int8(0)
	if val1.Type != 0 {
		typ = val1.Type
	}
	if val2.Type != 0 {
		if typ == 0 {
			typ = val2.Type
		} else if typ != val2.Type { // 两个都有类型信息但是类型不一致
			panic(fmt.Sprintf("type mismatch: %v != %v", val1.Type, val2.Type))
		}
	}
	switch typ {
	case TypInt:
		return Compare(val1.ToInt(), val2.ToInt())
	case TypFloat:
		return Compare(val1.ToFloat(), val2.ToFloat())
	case TypStr, TypTxt:
		return Compare(val1.ToStr(), val2.ToStr())
	default: // 没有类型信息或，类型不可比较
		panic(fmt.Sprintf("uncomparable type: %v", typ))
	}
}

func CalculateExpr(expr *ExprNode, columns []*Column, data []any) bool {
	left := ParseValue(expr.Left, columns, data)
	right := ParseValue(expr.Right, columns, data)
	switch expr.Operator {
	case EQ:
		return CompareValue(left, right) == 0
	case NE:
		return CompareValue(left, right) != 0
	case GT:
		return CompareValue(left, right) > 0
	case GE:
		return CompareValue(left, right) >= 0
	case LT:
		return CompareValue(left, right) < 0
	case LE:
		return CompareValue(left, right) <= 0
	case AND:
		return left.ToBool() && right.ToBool()
	case OR:
		return left.ToBool() || right.ToBool()
	default:
		panic(fmt.Sprintf("unsupport operator: %v", expr.Operator))
	}
}

func DistinctSlice[T comparable](val []T) []T {
	res := make([]T, 0)
	set := make(map[T]struct{})
	for _, item := range val {
		if _, ok := set[item]; !ok {
			set[item] = struct{}{}
			res = append(res, item)
		}
	}
	return res
}

func SubSlice[T comparable](val1 []T, val2 []T) []T {
	set := make(map[T]struct{})
	for _, item := range val2 {
		set[item] = struct{}{}
	}
	res := make([]T, 0)
	for _, item := range val1 {
		if _, ok := set[item]; !ok {
			res = append(res, item)
		}
	}
	return res
}

func GetFuncColumnName(node *FuncNode) string {
	buff := &strings.Builder{}
	buff.WriteString(node.FuncName)
	// 各拿一个特征拼一下，保证其唯一就行了
	for _, param := range node.Params {
		if idNode, ok := param.(*IDNode); ok {
			buff.WriteRune('#')
			buff.WriteString(idNode.Value)
		}
	}
	return buff.String()
}

func TokenTypeToType(tokenType string) int8 {
	switch tokenType {
	case INT:
		return TypInt
	case FLOAT:
		return TypFloat
	case STR:
		return TypStr
	default:
		panic(fmt.Sprintf("unknown token type: %s", tokenType))
	}
}

func PrintTable(operator IOperator) {
	data := make([][]string, 0)
	ls := make([]int, len(operator.GetColumns()))
	row := make([]string, 0)
	for i, column := range operator.GetColumns() {
		row = append(row, column.Name)
		ls[i] = max(ls[i], len(column.Name))
	}
	data = append(data, row)
	for {
		res := operator.Next()
		if res == nil {
			break
		}
		row = make([]string, 0)
		for i, item := range res {
			itemStr := fmt.Sprintf("%v", item)
			row = append(row, itemStr)
			ls[i] = max(ls[i], len(itemStr))
		}
		data = append(data, row)
	}
	buff := &strings.Builder{}
	buff.WriteRune('+')
	for _, l := range ls {
		for i := 0; i < l+2; i++ {
			buff.WriteRune('-')
		}
		buff.WriteRune('+')
	}
	item := buff.String()
	buff = &strings.Builder{}
	buff.WriteString(item)
	for _, row = range data {
		buff.WriteString("\n| ")
		for i, temp := range row {
			buff.WriteString(temp)
			for j := 0; j < ls[i]-len(temp); j++ {
				buff.WriteRune(' ')
			}
			buff.WriteString(" | ")
		}
		buff.WriteString("\n")
		buff.WriteString(item)
	}
	fmt.Println(buff.String())
}

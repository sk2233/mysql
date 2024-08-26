/*
@author: sk
@date: 2024/8/8
*/
package main

import (
	"fmt"
	"strconv"
	"strings"
)

type Parser struct {
	Tokens []*Token
	Idx    int
}

/*
select a,b from t1
select * from t2 where a > b
select a,b from t3 where a > c order by b
select a,count(*) from t4 where b > 100 group by a
select t1.name,t2.age from t1 left join t2 on t1.name = t2.name
update t2 set n = 22,a = 33 where a > 100 AND b = 100
insert into t3(a,b,z,d) values(2,3,2,4)
delete from t3 where a = 100
CREATE TABLE t2(uid int,name text)
CREATE INDEX idx ON t2(a,b)
*/

func (p *Parser) ParseTokens() INode {
	if p.Match(SELECT) {
		return p.parseSelect()
	}
	if p.Match(UPDATE) {
		return p.parseUpdate()
	}
	if p.Match(INSERT) {
		return p.parseInsert()
	}
	if p.Match(DELETE) {
		return p.parseDelete()
	}
	if p.Match(CREATE) {
		if p.Match(TABLE) {
			return p.parseCreateTable()
		}
		if p.Match(INDEX) {
			return p.parseCreateIndex()
		}
	}
	panic("unknown sql type")
}

func (p *Parser) parseColumn() *ColumnNode {
	res := &ColumnNode{}
	name := p.MustRead(ID)
	res.Name = name.Value
	typ := p.MustRead(INT, FLOAT, VARCHAR, TEXT)
	res.Type = strings.ToUpper(typ.Value)
	if p.Match(LPAREN) {
		temp := p.MustRead(INT)
		l, err := strconv.ParseInt(temp.Value, 10, 64)
		HandleErr(err)
		res.Len = l
		p.MustRead(RPAREN)
	}
	return res
}

func (p *Parser) parseCreateTable() INode {
	res := &CreateTableNode{}
	// table
	table := p.MustRead(ID)
	res.Table = table.Value
	// column
	p.MustRead(LPAREN)
	res.Columns = append(res.Columns, p.parseColumn())
	for p.Match(COMMA) {
		res.Columns = append(res.Columns, p.parseColumn())
	}
	p.MustRead(RPAREN)
	p.MustRead(EOF)
	return res
}

func (p *Parser) parseCreateIndex() INode {
	res := &CreateIndexNode{}
	// index
	index := p.MustRead(ID)
	res.Index = index.Value
	// table
	p.MustRead(ON)
	table := p.MustRead(ID)
	res.Table = table.Value
	// column
	p.MustRead(LPAREN)
	column := p.MustRead(ID)
	res.Columns = append(res.Columns, column.Value)
	for p.Match(COMMA) {
		column = p.MustRead(ID)
		res.Columns = append(res.Columns, column.Value)
	}
	p.MustRead(RPAREN)
	return res
}

func (p *Parser) parseDelete() INode {
	res := &DeleteNode{}
	p.MustRead(FROM)
	// table
	table := p.MustRead(ID)
	res.Table = table.Value
	// where
	if p.Match(WHERE) {
		res.Where = p.parseExpr()
	}
	p.MustRead(EOF)
	return res
}

func (p *Parser) parseInsert() INode {
	res := &InsertNode{}
	p.MustRead(INTO)
	// table
	table := p.MustRead(ID)
	res.Table = table.Value
	// values
	p.MustRead(VALUES)
	// 至少有一个
	p.MustRead(LPAREN)
	temp := make([]*Value, 0)
	val := p.MustRead(INT, FLOAT, STR)
	temp = append(temp, &Value{Value: val.Value})
	for p.Match(COMMA) {
		val = p.MustRead(INT, FLOAT, STR)
		temp = append(temp, &Value{Value: val.Value})
	}
	p.MustRead(RPAREN)
	res.Values = append(res.Values, temp)
	for p.Match(COMMA) {
		p.MustRead(LPAREN)
		temp = make([]*Value, 0)
		val = p.MustRead(INT, FLOAT, STR)
		temp = append(temp, &Value{Value: val.Value})
		for p.Match(COMMA) {
			val = p.MustRead(INT, FLOAT, STR)
			temp = append(temp, &Value{Value: val.Value})
		}
		p.MustRead(RPAREN)
		res.Values = append(res.Values, temp)
	}
	p.MustRead(EOF)
	return res
}

func (p *Parser) parseUpdate() INode {
	res := &UpdateNode{}
	// table
	table := p.MustRead(ID)
	res.Table = table.Value
	// Sets
	p.MustRead(SET)
	res.Sets = append(res.Sets, p.parseSet())
	for p.Match(COMMA) {
		res.Sets = append(res.Sets, p.parseSet())
	}
	// where
	if p.Match(WHERE) {
		res.Where = p.parseExpr()
	}
	p.MustRead(EOF)
	return res
}

func (p *Parser) parseSet() *SetNode {
	field := p.MustRead(ID)
	p.MustRead(EQ)
	token := p.Read()
	if token.Type == INT || token.Type == FLOAT || token.Type == STR {
		return &SetNode{
			Field: field.Value,
			Value: &ImmNode{Value: token.Value},
		}
	}
	if token.Type != ID {
		panic(fmt.Sprintf("token type %s not ID", token.Type))
	}
	if p.Match(LPAREN) {
		return &SetNode{
			Field: field.Value,
			Value: p.parseFunc(token),
		}
	}
	return &SetNode{
		Field: field.Value,
		Value: &IDNode{Value: token.Value},
	}
}

func (p *Parser) parseSelect() INode {
	res := &SelectNode{}
	// select
	if p.Match(DISTINCT) { // 有 DISTINCT 的话，select 就直接使用 DISTINCT 的列
		field := p.MustRead(ID)
		res.Distinct = append(res.Distinct, field.Value)
		for p.Match(COMMA) {
			field = p.MustRead(ID)
			res.Distinct = append(res.Distinct, field.Value)
		}
		for _, item := range res.Distinct {
			res.Fields = append(res.Fields, &IDNode{
				Value: item,
			})
		}
	} else {
		res.Fields = append(res.Fields, p.parseField())
		for p.Match(COMMA) {
			res.Fields = append(res.Fields, p.parseField())
		}
	}
	// from
	p.MustRead(FROM)
	table := p.MustRead(ID)
	res.From = table.Value
	// join
	if p.Match(JOIN) {
		table = p.MustRead(ID)
		p.MustRead(ON) // join 必须有条件
		res.Join = &JoinNode{
			Table:     table.Value,
			Condition: p.parseExpr(),
		}
	}
	// where
	if p.Match(WHERE) {
		res.Where = p.parseExpr()
	}
	// group by
	if p.Match(GROUP) {
		p.MustRead(BY)
		group := p.MustRead(ID)
		res.Groups = append(res.Groups, group.Value)
		for p.Match(COMMA) {
			group = p.MustRead(ID)
			res.Groups = append(res.Groups, group.Value)
		}
	}
	// order by
	if p.Match(ORDER) {
		p.MustRead(BY)
		res.Orders = append(res.Orders, p.parseOrder())
		for p.Match(COMMA) {
			res.Orders = append(res.Orders, p.parseOrder())
		}
	}
	// limit
	if p.Match(LIMIT) {
		temp := p.MustRead(INT)
		limit, err := strconv.ParseInt(temp.Value, 10, 64)
		HandleErr(err)
		res.Limit = &LimitNode{
			Limit: int(limit),
		}
		if p.Match(OFFSET) { // 依赖 limit 否则不能单独出现 offset
			temp = p.MustRead(INT)
			offset, err := strconv.ParseInt(temp.Value, 10, 64)
			HandleErr(err)
			res.Limit.Offset = int(offset)
		}
	}
	p.MustRead(EOF)
	return res
}

func (p *Parser) parseOrder() *OrderNode {
	field := p.MustRead(ID)
	desc := false
	if p.Match(ASC) {
		desc = false
	} else if p.Match(DESC) {
		desc = true
	}
	return &OrderNode{
		Field: field.Value,
		Desc:  desc,
	}
}

func (p *Parser) parseField() INode {
	token := p.Read()
	if token.Type == STAR {
		return &StarNode{}
	}
	if token.Type == INT || token.Type == FLOAT || token.Type == STR {
		return &ImmNode{Value: token.Value}
	}
	if token.Type != ID {
		panic(fmt.Sprintf("parseField err token %v not id", token.Type))
	}
	if p.Match(LPAREN) {
		return p.parseFunc(token)
	}
	return &IDNode{Value: token.Value}
}

func (p *Parser) parseParam() INode {
	token := p.Read()
	if token.Type == INT || token.Type == FLOAT || token.Type == STR {
		return &ImmNode{Value: token.Value}
	}
	if token.Type != ID {
		panic(fmt.Sprintf("parseParam err token %v not id", token.Type))
	}
	if p.Match(LPAREN) { // 支持函数嵌套
		return p.parseFunc(token)
	}
	return &IDNode{Value: token.Value}
}

func (p *Parser) parseExpr() *ExprNode {
	left := p.parseSubExpr()
	for {
		token := p.Read()
		if token.Type == AND || token.Type == OR {
			left = &ExprNode{
				Left:     left,
				Right:    p.parseSubExpr(),
				Operator: token.Value,
			}
		} else {
			p.UnRead()
			return left.(*ExprNode)
		}
	}
}

func (p *Parser) parseSubExpr() INode {
	left := p.parseExprItem()
	for {
		token := p.Read()
		if token.Type == EQ || token.Type == NE || token.Type == GT || token.Type == GE ||
			token.Type == LT || token.Type == LE {
			left = &ExprNode{
				Left:     left,
				Right:    p.parseExprItem(),
				Operator: token.Value,
			}
		} else {
			p.UnRead()
			return left
		}
	}
}

func (p *Parser) parseExprItem() INode {
	if p.Match(LPAREN) {
		item := p.parseExpr()
		p.MustRead(RPAREN)
		return item
	}
	token := p.Read()
	if token.Type == INT || token.Type == FLOAT || token.Type == STR {
		return &ImmNode{Value: token.Value}
	} else if token.Type == ID {
		if p.Match(LPAREN) {
			return p.parseFunc(token)
		} else {
			return &IDNode{Value: token.Value}
		}
	}
	panic(fmt.Sprintf("parseExpr err token %v type", token.Type))
}

func (p *Parser) parseFunc(token *Token) *FuncNode {
	params := make([]INode, 0)
	if !p.Match(RPAREN) {
		params = append(params, p.parseParam())
		for p.Match(COMMA) {
			params = append(params, p.parseParam())
		}
		p.MustRead(RPAREN)
	}
	return &FuncNode{
		FuncName: token.Value,
		Params:   params,
	}
}

func (p *Parser) Match(type0 string) bool {
	if p.Tokens[p.Idx].Type == type0 {
		p.Idx++
		return true
	}
	return false
}

func (p *Parser) Read() *Token {
	p.Idx++
	return p.Tokens[p.Idx-1]
}

func (p *Parser) MustRead(types ...string) *Token {
	token := p.Read()
	for _, type0 := range types {
		if token.Type == type0 {
			return token
		}
	}
	panic(fmt.Sprintf("MustRead err type %v not types %v", token.Type, types))
}

func (p *Parser) UnRead() {
	p.Idx--
}

func NewParser(tokens []*Token) *Parser {
	return &Parser{Tokens: tokens, Idx: 0}
}

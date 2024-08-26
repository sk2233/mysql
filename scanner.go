/*
@author: sk
@date: 2024/8/7
*/
package main

import (
	"fmt"
	"strings"
)

type Scanner struct {
	Sql string
	Idx int
}

func NewScanner(sql string) *Scanner {
	return &Scanner{Sql: sql, Idx: 0}
}

func (s *Scanner) ScanToken() *Token {
	ch := s.Read()
	switch ch {
	case '*':
		return NewToken(STAR, "*")
	case '.':
		return NewToken(DOT, ".")
	case ',':
		return NewToken(COMMA, ",")
	case '(':
		return NewToken(LPAREN, "(")
	case ')':
		return NewToken(RPAREN, ")")
	case '=':
		return NewToken(EQ, "=")
	case '!':
		s.MustMatch('=')
		return NewToken(NE, "!=")
	case '>':
		if s.Match('=') {
			return NewToken(GE, ">=")
		}
		return NewToken(GT, ">")
	case '<':
		if s.Match('=') {
			return NewToken(LE, "<=")
		}
		return NewToken(LT, "<")
	case '\'': // STR
		l := s.Idx
		for s.Sql[s.Idx] != '\'' {
			s.Idx++
		}
		r := s.Idx
		s.Idx++
		return NewToken(STR, s.Sql[l:r])
	case ' ', '\t', '\n', '\r': // 忽略这些字段
		return nil
	default:
		l := s.Idx - 1
		if IsDigit(ch) {
			isFloat := false // 支持整数与浮点数
			for s.HasMore() && IsDigit(s.Sql[s.Idx]) {
				s.Idx++
			}
			if s.HasMore() && s.Sql[s.Idx] == '.' {
				isFloat = true
				s.Idx++
			}
			for s.HasMore() && IsDigit(s.Sql[s.Idx]) {
				s.Idx++
			}
			if isFloat {
				return NewToken(FLOAT, s.Sql[l:s.Idx])
			} else {
				return NewToken(INT, s.Sql[l:s.Idx])
			}
		} else if IsAlpha(ch) {
			// id 中允许出现 .  例如 t1.name 算做一个
			for s.HasMore() && (IsAlpha(s.Sql[s.Idx]) || IsDigit(s.Sql[s.Idx]) || s.Sql[s.Idx] == '.') {
				s.Idx++
			}
			val := s.Sql[l:s.Idx]
			if type0, ok := Keywords[strings.ToUpper(val)]; ok {
				return NewToken(type0, val)
			}
			return NewToken(ID, val)
		} else {
			panic(fmt.Sprintf("unknown token type: %c", ch))
		}
	}
}

func (s *Scanner) ScanTokens() []*Token {
	tokens := make([]*Token, 0)
	for s.HasMore() {
		if token := s.ScanToken(); token != nil {
			tokens = append(tokens, token)
		}
	}
	tokens = append(tokens, NewToken(EOF, ""))
	return tokens
}

func (s *Scanner) Read() byte {
	res := s.Sql[s.Idx]
	s.Idx++
	return res
}

func (s *Scanner) MustMatch(val byte) {
	res := s.Read()
	if res != val {
		panic(fmt.Sprintf("invalid token need %c get %c", val, res))
	}
}

func (s *Scanner) Match(val byte) bool {
	if s.Sql[s.Idx] == val {
		s.Idx++
		return true
	}
	return false
}

func (s *Scanner) HasMore() bool {
	return s.Idx < len(s.Sql)
}

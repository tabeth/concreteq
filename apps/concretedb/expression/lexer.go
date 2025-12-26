package expression

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type TokenType int

const (
	TokenEOF TokenType = iota
	TokenError

	// Identifiers and Literals
	TokenIdentifier // Name, path part
	TokenValue      // :v1, :val

	// Operators
	TokenEq  // =
	TokenNE  // <>
	TokenLT  // <
	TokenLTE // <=
	TokenGT  // >
	TokenGTE // >=

	// Keywords
	TokenAND
	TokenOR
	TokenNOT
	TokenBETWEEN
	TokenIN
	TokenBeginsWith
	TokenAttributeExists
	TokenAttributeNotExists
	TokenContains
	TokenSize
	TokenListAppend
	TokenIfNotExists

	// Delimiters
	TokenLParen   // (
	TokenRParen   // )
	TokenComma    // ,
	TokenDot      // .
	TokenLBracket // [
	TokenRBracket // ]
	TokenSET
	TokenREMOVE
	TokenADD
	TokenDELETE
	TokenPlus
	TokenMinus
)

var keywords = map[string]TokenType{
	"AND":                  TokenAND,
	"OR":                   TokenOR,
	"NOT":                  TokenNOT,
	"BETWEEN":              TokenBETWEEN,
	"IN":                   TokenIN,
	"begins_with":          TokenBeginsWith,
	"attribute_exists":     TokenAttributeExists,
	"attribute_not_exists": TokenAttributeNotExists,
	"contains":             TokenContains,
	"size":                 TokenSize,
	"list_append":          TokenListAppend,
	"if_not_exists":        TokenIfNotExists,
	"SET":                  TokenSET,
	"REMOVE":               TokenREMOVE,
	"ADD":                  TokenADD,
	"DELETE":               TokenDELETE,
}

type Token struct {
	Type    TokenType
	Literal string
	Pos     int
}

func (t Token) String() string {
	return fmt.Sprintf("Token(%d, %q)", t.Type, t.Literal)
}

type Lexer struct {
	input  string
	start  int
	pos    int
	width  int
	tokens []Token
}

func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.run()
	return l
}

func (l *Lexer) run() {
	for state := lexText; state != nil; {
		state = state(l)
	}
}

func (l *Lexer) next() rune {
	if l.pos >= len(l.input) {
		l.width = 0
		return 0 // EOF
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = w
	l.pos += l.width
	return r
}

func (l *Lexer) backup() {
	l.pos -= l.width
}

func (l *Lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

func (l *Lexer) ignore() {
	l.start = l.pos
}

func (l *Lexer) emit(t TokenType) {
	l.tokens = append(l.tokens, Token{Type: t, Literal: l.input[l.start:l.pos], Pos: l.start})
	l.start = l.pos
}

func (l *Lexer) acceptRun(valid string) {
	for strings.ContainsRune(valid, l.next()) {
	}
	l.backup()
}

// State functions
type stateFn func(*Lexer) stateFn

func lexText(l *Lexer) stateFn {
	for {
		switch r := l.next(); {
		case r == 0: // EOF
			l.emit(TokenEOF)
			return nil
		case isSpace(r):
			l.ignore()
		case r == '+':
			l.emit(TokenPlus)
		case r == '-':
			l.emit(TokenMinus)
		case r == '=':
			l.emit(TokenEq)
		case r == '<':
			if l.peek() == '>' {
				l.next()
				l.emit(TokenNE)
			} else if l.peek() == '=' {
				l.next()
				l.emit(TokenLTE)
			} else {
				l.emit(TokenLT)
			}
		case r == '>':
			if l.peek() == '=' {
				l.next()
				l.emit(TokenGTE)
			} else {
				l.emit(TokenGT)
			}
		case r == '(':
			l.emit(TokenLParen)
		case r == ')':
			l.emit(TokenRParen)
		case r == ',':
			l.emit(TokenComma)
		case r == '.':
			l.emit(TokenDot)
		case r == '[':
			l.emit(TokenLBracket)
		case r == ']':
			l.emit(TokenRBracket)
		case r == ':':
			return lexValue
		case r == '#':
			// ExpressionAttributeName placeholder
			return lexIdentifier
		case isActiveIdentifierChar(r):
			l.backup()
			return lexIdentifier
		case unicode.IsDigit(r):
			l.backup()
			return lexNumber
		default:
			// Just emit error for now
			l.emit(TokenError)
			return nil
		}
	}
}

func lexNumber(l *Lexer) stateFn {
	l.acceptRun("0123456789")
	if l.peek() == '.' {
		l.next()
		l.acceptRun("0123456789")
	}
	// Treat numbers as TokenIdentifier for Parser compatibility
	l.emit(TokenIdentifier)
	return lexText
}

func lexValue(l *Lexer) stateFn {
	// :val
	for {
		r := l.next()
		if !isAlphaNumeric(r) && r != '_' && r != '-' {
			l.backup()
			break
		}
	}
	l.emit(TokenValue)
	return lexText
}

func lexIdentifier(l *Lexer) stateFn {
	// Simple identifier or keyword
	// Also handles #name
	for {
		r := l.next()
		if !isAlphaNumeric(r) && r != '_' && r != '-' && r != '#' {
			l.backup()
			break
		}
	}

	word := l.input[l.start:l.pos]
	if tok, ok := keywords[word]; ok {
		l.emit(tok)
	} else {
		l.emit(TokenIdentifier)
	}
	return lexText
}

func isSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\r' || r == '\n'
}

func isAlphaNumeric(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

func isActiveIdentifierChar(r rune) bool {
	return unicode.IsLetter(r) || r == '_'
}

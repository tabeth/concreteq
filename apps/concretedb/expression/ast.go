package expression

import "fmt"

type NodeType int

const (
	NodeBinary NodeType = iota
	NodeUnary
	NodeFunction
	NodeLiteral
	NodePath
	NodeParen
	NodeUpdate
)

type Node interface {
	Type() NodeType
	String() string
}

type BinaryNode struct {
	Left     Node
	Operator Token
	Right    Node
}

func (n *BinaryNode) Type() NodeType { return NodeBinary }
func (n *BinaryNode) String() string {
	return fmt.Sprintf("(%s %s %s)", n.Left, n.Operator.Literal, n.Right)
}

type UnaryNode struct {
	Operator Token
	Operand  Node
}

func (n *UnaryNode) Type() NodeType { return NodeUnary }
func (n *UnaryNode) String() string {
	return fmt.Sprintf("(%s %s)", n.Operator.Literal, n.Operand)
}

type FunctionNode struct {
	Name      string
	Arguments []Node
}

func (n *FunctionNode) Type() NodeType { return NodeFunction }
func (n *FunctionNode) String() string {
	return fmt.Sprintf("%s(%v)", n.Name, n.Arguments)
}

type LiteralNode struct {
	Value string
}

func (n *LiteralNode) Type() NodeType { return NodeLiteral }
func (n *LiteralNode) String() string { return n.Value }

type PathPart struct {
	Name    string
	Index   int
	IsIndex bool
}

type PathNode struct {
	Parts []PathPart
}

func (n *PathNode) Type() NodeType { return NodePath }
func (n *PathNode) String() string {
	return fmt.Sprintf("Path(%v)", n.Parts)
}

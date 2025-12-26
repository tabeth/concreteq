package expression

import "fmt"

type UpdateNode struct {
	SetActions    []*SetAction
	RemoveActions []*RemoveAction
	AddActions    []*AddAction
	DeleteActions []*DeleteAction
}

func (n *UpdateNode) Type() NodeType { return NodeUpdate }
func (n *UpdateNode) String() string {
	return fmt.Sprintf("Update(SET=%v, REMOVE=%v, ADD=%v, DELETE=%v)", n.SetActions, n.RemoveActions, n.AddActions, n.DeleteActions)
}

type SetAction struct {
	Path  Node
	Value Node
}

func (n *SetAction) String() string { return fmt.Sprintf("%s = %s", n.Path, n.Value) }

type RemoveAction struct {
	Path Node
}

func (n *RemoveAction) String() string { return fmt.Sprintf("%s", n.Path) }

type AddAction struct {
	Path  Node
	Value Node
}

func (n *AddAction) String() string { return fmt.Sprintf("%s %s", n.Path, n.Value) }

type DeleteAction struct {
	Path  Node
	Value Node
}

func (n *DeleteAction) String() string { return fmt.Sprintf("%s %s", n.Path, n.Value) }

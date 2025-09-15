package directory

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNode_getPartitionSubpath(t *testing.T) {
	n := &node{
		path:       []string{"a", "b"},
		targetPath: []string{"a", "b", "c", "d"},
	}
	assert.Equal(t, []string{"c", "d"}, n.getPartitionSubpath())

	n = &node{
		path:       []string{"a", "b"},
		targetPath: []string{"a", "b"},
	}
	assert.Equal(t, []string{}, n.getPartitionSubpath())
}

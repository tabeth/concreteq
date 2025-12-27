package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tabeth/concreteq/models"
)

func TestBuildResponseAttributes(t *testing.T) {
	msg := &models.Message{
		ReceivedCount:  5,
		FirstReceived:  1678886400,
		SentTimestamp:  1678886400,
		SenderId:       "sender1",
		MessageGroupId: "group1",
		SequenceNumber: 123,
	}
	req := &models.ReceiveMessageRequest{
		AttributeNames:              []string{"All"},
		MessageSystemAttributeNames: []string{},
	}

	attrs := (&FDBStore{}).buildResponseAttributes(msg, req)
	assert.Equal(t, "5", attrs["ApproximateReceiveCount"])
	assert.Equal(t, "1678886400000", attrs["ApproximateFirstReceiveTimestamp"])
	assert.Equal(t, "1678886400000", attrs["SentTimestamp"])
	assert.Equal(t, "sender1", attrs["SenderId"])
	assert.Equal(t, "group1", attrs["MessageGroupId"])
	assert.Equal(t, "123", attrs["SequenceNumber"])
}

func TestBuildResponseMessageAttributes(t *testing.T) {
	sv := "value"
	msg := &models.Message{
		Attributes: map[string]models.MessageAttributeValue{
			"attr1": {DataType: "String", StringValue: &sv},
			"attr2": {DataType: "String", StringValue: &sv},
		},
	}
	req := &models.ReceiveMessageRequest{
		MessageAttributeNames: []string{"attr1"},
	}

	attrs := (&FDBStore{}).buildResponseMessageAttributes(msg, req)
	assert.Len(t, attrs, 1)
	assert.Contains(t, attrs, "attr1")

	req.MessageAttributeNames = []string{"All"}
	attrs = (&FDBStore{}).buildResponseMessageAttributes(msg, req)
	assert.Len(t, attrs, 2)
}

func TestHashAttributes(t *testing.T) {
	sv := "value"
	attrs := map[string]models.MessageAttributeValue{
		"attr1": {DataType: "String", StringValue: &sv},
		"attr2": {DataType: "Binary", BinaryValue: []byte("value2")},
	}
	hash := hashAttributes(attrs, []string{"attr1"})
	assert.NotEmpty(t, hash)
}

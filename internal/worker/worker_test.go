package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockProcessor struct {
	called    bool
	paymentID string
	err       error
}

func (m *mockProcessor) ProcessPayment(_ context.Context, paymentID string) error {
	m.called = true
	m.paymentID = paymentID
	return m.err
}

func makeTask(paymentID string) *asynq.Task {
	payload, _ := json.Marshal(paymentTask{PaymentID: paymentID})
	return asynq.NewTask("payment:process", payload)
}

func TestHandlePayment_Success(t *testing.T) {
	proc := &mockProcessor{}
	handler := HandlePayment(proc)

	err := handler(context.Background(), makeTask("pay-123"))
	require.NoError(t, err)
	assert.True(t, proc.called)
	assert.Equal(t, "pay-123", proc.paymentID)
}

func TestHandlePayment_PropagatesError(t *testing.T) {
	proc := &mockProcessor{err: errors.New("provider down")}
	handler := HandlePayment(proc)

	err := handler(context.Background(), makeTask("pay-456"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "provider down")
	assert.True(t, proc.called)
	assert.Equal(t, "pay-456", proc.paymentID)
}

func TestHandlePayment_BadPayload(t *testing.T) {
	proc := &mockProcessor{}
	handler := HandlePayment(proc)

	badTask := asynq.NewTask("payment:process", []byte("not json"))
	err := handler(context.Background(), badTask)
	require.Error(t, err)
	assert.False(t, proc.called)
}

func BenchmarkHandlePayment(b *testing.B) {
	proc := &mockProcessor{}
	handler := HandlePayment(proc)
	task := makeTask("pay-benchmark-id")
	ctx := context.Background()

	b.ResetTimer()
	for b.Loop() {
		_ = handler(ctx, task)
	}
}

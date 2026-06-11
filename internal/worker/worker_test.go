package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

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

func TestPaymentRetryDelay(t *testing.T) {
	// n = 0: baseDelay = 15s. Jitter is ±10% (±1.5s), so range is [13.5s, 16.5s]
	delay0 := PaymentRetryDelay(0, nil, nil)
	assert.GreaterOrEqual(t, delay0, 13*time.Second)
	assert.LessOrEqual(t, delay0, 17*time.Second)

	// n = 1: 15s * 2^1 = 30s. Jitter is ±10% (±3s), so range is [27s, 33s]
	delay1 := PaymentRetryDelay(1, nil, nil)
	assert.GreaterOrEqual(t, delay1, 27*time.Second)
	assert.LessOrEqual(t, delay1, 33*time.Second)

	// n = 2: 15s * 2^2 = 60s. Jitter is ±10% (±6s), so range is [54s, 66s]
	delay2 := PaymentRetryDelay(2, nil, nil)
	assert.GreaterOrEqual(t, delay2, 54*time.Second)
	assert.LessOrEqual(t, delay2, 66*time.Second)

	// n = 15: exponential exceeds maxDelay (8h). Capped at 8h. Jitter is ±10% (±48m), so range is [7.2h, 8.8h]
	delay15 := PaymentRetryDelay(15, nil, nil)
	assert.GreaterOrEqual(t, delay15, 7*time.Hour+10*time.Minute)
	assert.LessOrEqual(t, delay15, 8*time.Hour+50*time.Minute)
}

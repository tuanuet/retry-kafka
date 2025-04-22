package retriable

import (
	"testing"
	"time"
)

func TestNewTopic_Default(t *testing.T) {
	topic := NewTopic("main-topic")
	if topic.Name != "main-topic" {
		t.Errorf("Expected Name to be 'main-topic', got %s", topic.Name)
	}
	if topic.Pending != 0 {
		t.Errorf("Expected Pending to be 0, got %v", topic.Pending)
	}
	if topic.Next != nil {
		t.Errorf("Expected Next to be nil, got %v", topic.Next)
	}
}

func TestNewTopic_WithPending(t *testing.T) {
	pending := 5 * time.Second
	topic := NewTopic("retry-topic", WithPending(pending))
	if topic.Pending != pending {
		t.Errorf("Expected Pending to be %v, got %v", pending, topic.Pending)
	}
}

func TestWithPending_Mutate(t *testing.T) {
	topic := &Topic{Name: "t1"}
	WithPending(10 * time.Second)(topic)
	if topic.Pending != 10*time.Second {
		t.Errorf("Expected Pending to be 10s, got %v", topic.Pending)
	}
}

func TestTopic_Next(t *testing.T) {
	next := NewTopic("next-topic")
	topic := NewTopic("main-topic")
	topic.Next = next
	if topic.Next != next {
		t.Errorf("Expected Next to be %v, got %v", next, topic.Next)
	}
}

func TestTopic_Next_MultiLevel(t *testing.T) {
	main := NewTopic("main")
	retry1 := NewTopic("retry1")
	retry2 := NewTopic("retry2")
	main.Next = retry1
	retry1.Next = retry2

	if main.Next != retry1 {
		t.Errorf("Expected main.Next to be retry1")
	}
	if main.Next.Next != retry2 {
		t.Errorf("Expected main.Next.Next to be retry2")
	}
	if main.Next.Next.Next != nil {
		t.Errorf("Expected main.Next.Next.Next to be nil")
	}
}

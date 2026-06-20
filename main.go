package main

import (
	"fmt"
	"time"
)

// Mocking the structure of a pending message in NATS JetStream
type pendingMsg struct {
	ackDeadline time.Time
}

// processAck handles the acknowledgement logic
func processAck(msg *pendingMsg, ackType string, ackWait time.Duration) {
	switch ackType {
	case "+ACK":
		fmt.Println("Message acknowledged, cleaning up.")
	case "+WIP":
		// FIX: Reset the AckWait timer
		msg.ackDeadline = time.Now().Add(ackWait)
		fmt.Printf("In-Progress ack received. Deadline extended to %v\n", msg.ackDeadline)
	case "-NAK":
		fmt.Println("Negative ack received, triggering redelivery.")
	}
}

func main() {
	ackWait := 2 * time.Second
	msg := &pendingMsg{ackDeadline: time.Now().Add(ackWait)}
	
	fmt.Println("Initial deadline:", msg.ackDeadline)
	processAck(msg, "+WIP", ackWait)
}
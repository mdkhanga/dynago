package handlers

import (
	"github.com/mdkhanga/dynago/cluster"
	pb "github.com/mdkhanga/dynago/kvmessages"
	"github.com/mdkhanga/dynago/logger"
)

var (
	Log = logger.WithComponent("grpcserver").Log
)

type MessageHandler func(msg *pb.ServerMessage, p *cluster.Peer) *pb.ServerMessage

// HandlerRegistry maintains a map of message types to handler functions.
type HandlerRegistry struct {
	handlers map[pb.MessageType]MessageHandler
}

// NewHandlerRegistry creates a new HandlerRegistry.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[pb.MessageType]MessageHandler),
	}
}

// RegisterHandler registers a handler function for a specific message type.
func (r *HandlerRegistry) RegisterHandler(messageType pb.MessageType, handler MessageHandler) {
	r.handlers[messageType] = handler
}

// HandleMessage looks up the handler for the given message type and calls it.
func (r *HandlerRegistry) HandleMessage(msg *pb.ServerMessage, p *cluster.Peer) *pb.ServerMessage {
	handler, exists := r.handlers[msg.Type]
	if !exists {
		Log.Info().Any("No handler registered for message type: %s\n", msg.Type).Send()
		return nil
	}
	return handler(msg, p)
}

func (r *HandlerRegistry) Init() {

	r.RegisterHandler(pb.MessageType_PING)

}

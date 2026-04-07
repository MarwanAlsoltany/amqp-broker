package broker

import (
	"github.com/MarwanAlsoltany/amqp-broker/internal/transport"
)

var (
	// ErrConnection is the base error for connection operations.
	// All connection-related errors wrap this error.
	ErrConnection = transport.ErrConnection

	// ErrConnectionClosed indicates the connection is closed.
	// This error is returned when operations are attempted on a closed connection.
	ErrConnectionClosed = transport.ErrConnectionClosed

	// ErrConnectionManager is the base error for connection manager operations.
	// All connection manager errors wrap this error.
	ErrConnectionManager = transport.ErrConnectionManager

	// ErrConnectionManagerClosed indicates the connection manager is closed.
	// This error is returned when operations are attempted on a closed manager.
	ErrConnectionManagerClosed = transport.ErrConnectionManagerClosed

	// ErrChannel is the base error for channel operations.
	// All channel-related errors wrap this error.
	ErrChannel = transport.ErrChannel

	// ErrChannelClosed indicates the channel is closed.
	// This error is returned when operations are attempted on a closed channel.
	ErrChannelClosed = transport.ErrChannelClosed
)

type (
	// Config holds AMQP connection configuration parameters
	// such as heartbeat interval, locale, and SASL mechanisms.
	Config = transport.Config

	// Dialer is a function type for creating AMQP connections.
	// It enables dependency injection for testing and custom connection logic.
	Dialer = transport.Dialer

	// Arguments represents AMQP arguments used in declarations.
	// It's an alias for map[string]interface{} with AMQP type encoding.
	Arguments = transport.Arguments

	// Connection represents an AMQP connection.
	// It provides methods for managing channels and connection lifecycle.
	Connection = transport.Connection

	// Channel represents an AMQP channel within a connection.
	// It provides methods for publishing, consuming, and topology operations.
	Channel = transport.Channel

	// AMQPError represents an AMQP protocol error returned by the server.
	// It contains the error code, text, class ID, and method ID.
	AMQPError = transport.Error
)

var (
	// DefaultDialer is the default function used to create AMQP connections.
	DefaultDialer Dialer = transport.DefaultDialer
)

type (
	// ConnectionManagerOptions configures the connection manager.
	// It includes pool size, reconnection strategy, and lifecycle hooks.
	ConnectionManagerOptions = transport.ConnectionManagerOptions

	// ConnectionOnOpenHandler is called when a connection is (re-)established.
	// Parameters: idx (connection pool index)
	ConnectionOnOpenHandler = transport.ConnectionOnOpenHandler

	// ConnectionOnCloseHandler is called when a connection closes.
	// Parameters: idx (connection pool index), code (AMQP error code), reason (error description),
	// server (true if initiated by server), recover (true if recoverable)
	ConnectionOnCloseHandler = transport.ConnectionOnCloseHandler

	// ConnectionOnBlockHandler is called when connection flow control changes.
	// Parameters: idx (connection pool index), active (true=blocked, false=unblocked),
	// reason (blocking reason, only set when active=true)
	ConnectionOnBlockHandler = transport.ConnectionOnBlockHandler
)

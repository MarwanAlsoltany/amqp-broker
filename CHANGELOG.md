# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-04-16

### Added

- **broker**
  - Add context injection utilities
  - Add transport package types and errors exports
  - Add Option type and configuration helpers
  - Add ErrTransport for transport operation errors
  - Add ErrHandler for transport operation errors
- **endpoint**
  - Add Endpoint interface and base endpoint
  - Implement endpoint lifecycle management
  - Implement AMQP message conversion helpers
  - Add Publisher interface and implement publisher
  - Add Consumer interface and implement consumer
- **handler**
  - Add Handler and Middleware types
  - Implement recovery middlewares
  - Implement logging middlewares
  - Implement message middlewares
  - Implement retry middlewares
  - Implement flow middlewares
  - Implement batch middleware
- **internal**
  - Add internal package for shared types and utilities
  - Add internal errors for base error types
- **message**
  - Implement Message type
  - Implement Builder type
- **testing**
  - Add test utilities package
- **topology**
  - Add shared type aliases
  - Implement Exchange AMQP entity
  - Implement Queue AMQP entity
  - Implement Binding AMQP entity
  - Implement RoutingKey type
  - Implement Topology DTO type
  - Implement Registry type
- **transport**
  - Add Connection and Channel interfaces
  - Add amqp091-go type adapters
  - Implement ConnectionManager with Dialer abstraction and connection pooling
  - Implement options default, merge, and validate helpers
  - Implement safe channel action helpers
  - Add package documentation

### Changed

- **broker**
  - Migrate to internal packages and remove old implementations
  - Update top-level package test infrastructure
- **endpoint**
  - Migrate to internal/endpoint and remove top-level implementation
- **errors**
  - Replace hand-rolled error type with serrors adapter
- **handler**
  - Migrate to internal/handler and remove top-level implementation
- **message**
  - Migrate to internal/message and remove top-level implementation
- **topology**
  - Migrate to internal/topology and remove top-level implementation
- **transport**
  - Migrate error sentinels and call sites to serrors

### Dependencies

- **deps**
  - Add serrors as dependency
  - Update testcontainers and integration test dependencies

### Documentation

- **broker**
  - Update package documentation
  - Overhaul package documentation

### CI/CD

- **ci**
  - Add CI workflow
  - Add Dependabot config
  - Add release workflow

## [0.1.0] - 2026-03-28

### Added

- **broker**
  - Add connection event handlers and Config integration
  - Implement endpoint caching system for performance optimization
- **config**
  - Add default cache TTL constant
- **connection**
  - Implement connection pooling and resource management
  - Add event handlers and Config support
  - Add ConnectionManagerOptions type for configuration
- **consumer**
  - Enhance consumer with middleware support and message handling
- **endpoint**
  - Create endpoint abstraction interface
  - Add validation for PublisherOptions and ConsumerOptions
  - Add NoAutoDeclare option to EndpointOptions
  - Implement NoAutoDeclare for publisher and consumer
- **errors**
  - Centralize error definitions and add structured error handling
- **handler**
  - Add middleware handlers for logging and retry logic
  - Enhance middleware system with logging, metrics, and utilities
- **message**
  - Create message struct for AMQP message handling
  - Enhance message with additional fields and helpers
  - Add ContentEncoding support to Message struct
- **pool**
  - Implement generic TTL-based pooling system
- **topology**
  - Add helper methods and declarative topology management
- **types**
  - Add AMQP Config type and update type aliases

### Changed

- **broker**
  - Update broker for connection manager architecture
  - Integrate pool and topologyManager for improved architecture
  - Update for new error hierarchy and improved structure
  - Update message, defaults, and types for new architecture
  - Improve broker and connection manager structure and clarity
  - Improve initialization and resource management
  - Update broker to use ConnectionManagerOptions
- **builder**
  - Update message builder for consistency
- **config**
  - Move constants to defaults.go for better organization
  - Rename defaults.go to config.go for clarity
- **connection**
  - Replace connection pooling with connection manager
  - Update error handling in connection manager
  - Improve code clarity with better naming and structure
  - Streamline connection manager code
- **consumer**
  - Update consumer for new connection management
  - Update for new error handling and event system
  - Rename waitgroup field for clarity
  - Improve cancellation handling and error reporting
- **endpoint**
  - Update for declarative topology management
  - Update publisher and consumer for new architecture
  - Simplify endpoint base with topologyManager integration
  - Update consumer and publisher for new error and handler system
  - Update endpoint base and pool for consistency
  - Major refactor with EndpointOptions and improved documentation
  - Update consumer and publisher with improved options and docs
  - Improve reconnection logic and code clarity
- **errors**
  - Expand and reorganize error definitions
  - Implement hierarchical error structure with error wrapping
  - Add new error types and update type definitions
  - Simplify error hierarchy and improve error wrapping
  - Improve error message formatting
- **handler**
  - Rename Handler types and move middlewares to handler.go
  - Update handler middleware naming and documentation
  - Update handler naming for consistency
- **message**
  - Enhance message API with better methods and documentation
- **pool**
  - Improve pool implementation and error handling
- **publisher**
  - Improve publisher structure and message publishing logic
  - Update publisher for new connection management
  - Enhance error handling and flow control
- **topology**
  - Implement topologyManager with thread-safe operations
  - Rename Pattern to Key and improve code organization
  - Improve topology manager code organization
  - Improve code formatting and readability
- **types**
  - Consolidate and simplify type definitions
- **utils**
  - Clean up utility functions and remove unused code
  - Remove resource pooling utilities
  - Enhance utility functions and builder for improved consistency
  - Add utility functions for better code reuse

### Dependencies

- **deps**
  - Add testcontainers and integration test dependencies

### Documentation

- **broker**
  - Add package documentation for broker
  - Update package documentation
  - Enhance package documentation with detailed examples

[1.0.0]: https://github.com/MarwanAlsoltany/amqp-broker/compare/v0.1.0...v1.0.0
[0.1.0]: https://github.com/MarwanAlsoltany/amqp-broker/releases/tag/v0.1.0

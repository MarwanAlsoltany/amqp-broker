module github.com/MarwanAlsoltany/amqp-broker/examples

go 1.25.0

replace github.com/MarwanAlsoltany/amqp-broker => ../

require (
	github.com/MarwanAlsoltany/amqp-broker v0.0.0-00010101000000-000000000000
	github.com/rabbitmq/amqp091-go v1.10.0
)

require github.com/MarwanAlsoltany/serrors v0.1.0 // indirect

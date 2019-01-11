// Copyright 2018 REKTRA Network, All Rights Reserved.

package mqclient

import (
	"github.com/streadway/amqp"
)

type amqpMessage struct{ message amqp.Delivery }

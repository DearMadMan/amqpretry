## Overview

![diagram](diagram.png)

## Install

```bash
go get -u github.com/DearMadMan/amqpretry
```

## Usage

```go
op := amqpretry.Option{
    DNS: "amqp://dev:dev@localhost:15666",
    DeliverQueue: "deliver_queue",
    FailureQueue: "deliver_failure_queue",
    DeadLetterQueue: "deliver_dead_letter_queue",
    DeadLetterExchange: "dead_letter_exchange",
    InitQueueAndExchange: true,
    Runnable:  func(d *amqp.Delivery, retry *amqpretry.AMQPRetry) error {
        // no need to 'ack' or 'nack' messages
        // message will retry when error returned and policy allowed

        return nil
    }, 
}

retry, err := amqpretry.New(op)
if err != nil {
    log.Fatal(err)
}

retry.Start()
```

## Reference
[lanetix/node-amqplib-retry](https://github.com/lanetix/node-amqplib-retry)

## Feature

* [x] OnAckError
* [x] AfterAck
* [x] ErrorHeader
* [x] Rabbitmq lazy mode header support
* [x] Message Qos

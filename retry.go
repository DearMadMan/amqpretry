package amqpretry

import (
	"errors"
	"log"
	"math"
	"strconv"
	"sync"

	"github.com/streadway/amqp"
)

const (
	RetryHeader = "retry-times"
	ErrorHeader = "error-message"
)

type AMQPRetry struct {
	option       Option
	amqpConn     *amqp.Connection
	consumer     *amqp.Channel
	consumerOnce sync.Once
	pusher       *amqp.Channel
	pusherOnce   sync.Once
}

type Option struct {
	DNS                  string
	DeliverQueue         string
	FailureQueue         string
	DeadLetterQueue      string
	DeadLetterExchange   string
	RetryHeader          string
	EnableErrorHeader    bool
	ErrorHeader          string
	Qos                  ConsumerQosOption
	InitQueueAndExchange bool
	DeclareLazyQueue     bool
	Runnable             func(d *amqp.Delivery, retry *AMQPRetry) error
	RetryPolicy          func(times int16) (bool, string)
	RetryHandle          func(d *amqp.Delivery, retry *AMQPRetry, next int16, expiration string) error
	FailureHandle        func(d *amqp.Delivery, retry *AMQPRetry) error
	OnAckError           func(d *amqp.Delivery, retry *AMQPRetry, err error)
	AfterAck             func(d *amqp.Delivery, retry *AMQPRetry)
}

type ConsumerQosOption struct {
	PrefetchCount int
	PrefetchSize  int
	Global        bool
}

func New(op Option) (*AMQPRetry, error) {
	err := validate(op)
	if err != nil {
		return nil, err
	}

	conn, err := amqp.Dial(op.DNS)
	if err != nil {
		return nil, err
	}

	retry := &AMQPRetry{
		option:   op,
		amqpConn: conn,
	}

	return retry, retry.init()
}

func validate(op Option) error {
	if op.DeliverQueue == "" {
		return errors.New("DeliverQueue empty")
	}

	if op.FailureQueue == "" {
		return errors.New("FailureQueue empty")
	}

	if op.DeadLetterExchange == "" {
		return errors.New("DeadLetterExchange empty")
	}

	if op.DeadLetterQueue == "" {
		return errors.New("DeadLetterQueue empty")
	}

	return nil
}

func (r *AMQPRetry) Start() {
	err := r.Consumer().Qos(r.option.Qos.PrefetchCount, r.option.Qos.PrefetchSize, r.option.Qos.Global)
	failOnError(err)

	messages, err := r.Consumer().Consume(
		r.option.DeliverQueue, // queue
		"",                    // consumer
		false,                 // auto-ack
		false,                 // exclusive
		false,                 // no-local
		false,                 // no-wait
		nil,                   // args
	)

	failOnError(err)

	for d := range messages {
		go func(d amqp.Delivery) {
			err := r.option.Runnable(&d, r)
			if err != nil {
				if r.option.EnableErrorHeader {
					r.setErrorHeader(&d, err)
				}
				next := r.currentRetryTimes(&d) + 1
				if ok, expiration := r.option.RetryPolicy(next); ok {
					err = r.option.RetryHandle(&d, r, next, expiration)
				} else {
					err = r.option.FailureHandle(&d, r)
				}
				failOnError(err)
			}

			err = d.Ack(false)
			if err != nil {
				r.option.OnAckError(&d, r, err)
			} else {
				r.option.AfterAck(&d, r)
			}
		}(d)
	}
}

func (r *AMQPRetry) setErrorHeader(d *amqp.Delivery, err error) {
	if d.Headers == nil {
		d.Headers = make(amqp.Table)
	}

	d.Headers[r.option.ErrorHeader] = err.Error()
}

func (r *AMQPRetry) Pusher() *amqp.Channel {
	r.pusherOnce.Do(func() {
		ch, err := r.amqpConn.Channel()
		failOnError(err)
		r.pusher = ch
	})

	return r.pusher
}

func (r *AMQPRetry) Consumer() *amqp.Channel {
	r.consumerOnce.Do(func() {
		ch, err := r.amqpConn.Channel()
		failOnError(err)
		r.consumer = ch
	})

	return r.consumer
}

func (r *AMQPRetry) policy(times int16) (bool, string) {
	delay := math.Pow(2, float64(times))
	if delay > 60*60*24*5 {
		return false, "0"
	}
	return true, strconv.Itoa(int(delay * 1000))
}

func (r *AMQPRetry) retry(d *amqp.Delivery, retry *AMQPRetry, next int16, expiration string) error {
	if d.Headers == nil {
		d.Headers = make(amqp.Table)
	}
	d.Headers[r.option.RetryHeader] = next

	return r.Pusher().Publish(
		"",                       // exchange
		r.option.DeadLetterQueue, // routing key
		false,                    // mandatory
		false,                    // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Headers:      d.Headers,
			ContentType:  "application/json",
			Body:         d.Body,
			Expiration:   expiration,
		},
	)
}

func (r *AMQPRetry) onAckError(d *amqp.Delivery, retry *AMQPRetry, err error) {
	failOnError(err)
}

func (r *AMQPRetry) afterAck(d *amqp.Delivery, retry *AMQPRetry) {
}

func (r *AMQPRetry) failure(d *amqp.Delivery, retry *AMQPRetry) error {
	return r.Pusher().Publish(
		"",                    // exchange
		r.option.FailureQueue, // routing key
		false,                 // mandatory
		false,                 // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         d.Body,
		},
	)
}

func (r *AMQPRetry) currentRetryTimes(d *amqp.Delivery) int16 {
	times, ok := d.Headers[r.option.RetryHeader]
	if !ok {
		return 0
	}
	return times.(int16)
}

func (r *AMQPRetry) init() error {

	if r.option.RetryHeader == "" {
		r.option.RetryHeader = RetryHeader
	}

	if r.option.ErrorHeader == "" {
		r.option.ErrorHeader = ErrorHeader
	}

	if r.option.RetryPolicy == nil {
		r.option.RetryPolicy = r.policy
	}

	if r.option.RetryHandle == nil {
		r.option.RetryHandle = r.retry
	}

	if r.option.FailureHandle == nil {
		r.option.FailureHandle = r.failure
	}

	if r.option.OnAckError == nil {
		r.option.OnAckError = r.onAckError
	}

	if r.option.AfterAck == nil {
		r.option.AfterAck = r.afterAck
	}

	if r.option.InitQueueAndExchange {
		err := r.initQueueAndExchange()
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *AMQPRetry) initQueueAndExchange() error {
	queues := []string{
		r.option.DeliverQueue,
		r.option.FailureQueue,
	}

	ch, err := r.amqpConn.Channel()
	if err != nil {
		return err
	}

	for _, q := range queues {
		var priority int16 = 10
		args := make(amqp.Table)
		args["x-max-priority"] = priority
		if r.option.DeclareLazyQueue {
			args["x-queue-mode"] = "lazy"
		}

		_, err := ch.QueueDeclare(
			q,     // name
			true,  // durable
			false, // delete when unused
			false, // exclusive
			false, // no-wait
			args,
		)

		if err != nil {
			return err
		}
	}

	// Declare Dead letter Exchange
	err = ch.ExchangeDeclare(r.option.DeadLetterExchange, "direct", true, false, false, false, nil)
	if err != nil {
		return err
	}

	args := make(amqp.Table)
	args["x-dead-letter-exchange"] = r.option.DeadLetterExchange
	args["x-dead-letter-routing-key"] = r.option.DeliverQueue
	_, err = ch.QueueDeclare(r.option.DeadLetterQueue, true, false, false, false, args)
	if err != nil {
		return err
	}

	return ch.QueueBind(r.option.DeliverQueue, r.option.DeliverQueue, r.option.DeadLetterExchange, false, nil)
}

func failOnError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

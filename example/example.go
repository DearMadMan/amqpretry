package example

import (
	"fmt"
	"gpm/util"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/DearMadMan/amqpretry"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Worker struct {
	name string
}

func NewWorker() *Worker {
	return &Worker{name: "worker.name"}
}

func (w *Worker) Run() {
	op := amqpretry.Option{
		DNS:                  "amqp://dev:dev@localhost:5666",
		DeliverQueue:         "workers",
		FailureQueue:         "workers.failure",
		DeadLetterQueue:      "workers.retry",
		DeadLetterExchange:   "dead_letter_exchange",
		InitQueueAndExchange: true,
		Runnable:             w.run,
		RetryPolicy:          w.policy,
	}

	retry, err := amqpretry.New(op)
	util.FatalOnError(err)

	retry.Start()
}

func (w *Worker) run(d *amqp.Delivery, retry *amqpretry.AMQPRetry) error {
	type Body struct {
		URL string `json:"url"`
	}

	url := string(d.Body)
	logrus.Infof("[%s] run Worker: %s", w.name, url)

	client := &http.Client{
		Timeout: 3 * time.Minute,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).Dial,
		},
	}

	data := Body{URL: url}
	req, err := http.NewRequest("POST", "http://localhost:3000/", util.ToReaderFromObject(data))
	req.Header.Add("Content-Type", "application/json")
	util.FatalOnError(err)

	res, err := client.Do(req)
	if err != nil {
		logrus.Errorf("client do error: %s", err.Error())
		return err
	}

	defer res.Body.Close()
	rd, _ := ioutil.ReadAll(res.Body)
	logrus.Infof("Code: %d, Response: %s", res.StatusCode, string(rd))

	if res.StatusCode != 200 {
		return fmt.Errorf("res.StatusCode: %d, not 200", res.StatusCode)
	}

	return nil
}

func (w *Worker) policy(times int16) (bool, string) {
	p := map[int16]string{
		1: "30000",
		2: "300000",
		3: "1800000",
	}

	v, ok := p[times]
	return ok, v
}

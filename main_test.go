/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats"
)

func wait(ch chan bool) error {
	return waitTime(ch, 500*time.Millisecond)
}

func waitTime(ch chan bool, timeout time.Duration) error {
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
	}
	return errors.New("timeout")
}

func TestInstancesCreateBasic(t *testing.T) {
	os.Setenv("NATS_URI", "nats://localhost:4222")
	os.Setenv("REDIS_ADDR", "localhost:6379")

	n := natsClient()
	r := redisClient()

	processRequest(n, r, "executions.create", "execution.create")

	ch := make(chan bool)

	n.Subscribe("execution.create", func(m *nats.Msg) {
		event := &executionEvent{}
		json.Unmarshal(m.Data, event)
		if event.Type == "execution.create" {
			log.Println("Message Received")
			if event.Name != "name" {
				t.Fatal("Invalid name")
			}
			if event.ServiceName != "service_name" {
				t.Fatal("Invalid service name")
			}
			if event.ServiceType != "service_type" {
				t.Fatal("Invalid service type")
			}
			if event.ServiceEndPoint != "endpoint" {
				t.Fatal("Invalid service endpoint")
			}
			if event.ServiceOptions.User != "user" {
				t.Fatal("Invalid user")
			}
			if event.ServiceOptions.Password != "password" {
				t.Fatal("Invalid password")
			}
			if event.ExecutionType != "type" {
				t.Fatal("Invalid execution type")
			}
			if event.ExecutionPayload != "payload" {
				t.Fatal("Invalid payload")
			}
			if event.ExecutionTarget != "target" {
				t.Fatal("Invalid catalog type")
			}
			var key bytes.Buffer
			key.WriteString("GPBExecutions_")
			key.WriteString(event.Service)
			message, _ := r.Get(key.String()).Result()
			stored := &ExecutionsCreate{}
			err := json.Unmarshal([]byte(message), stored)
			if err != nil {
				log.Println(err)
			}
			if stored.Service != event.Service {
				t.Fatal("Event is not persisted correctly")
			}
			ch <- true
		} else {
			log.Println(event.Type)
			t.Fatal("Invalid received type")
		}
	})

	message := []byte(`{"service":"service","service_name":"service_name","service_type":"service_type","options":{"user":"user","password":"password"},"service_endpoint":"endpoint","executions":[{"name":"name", "datacenter": "test", "datacenter_name": "test", "datacenter_type": "vcloud", "datacenter_region": "LON-001", "datacenter_username": "test@test", "datacenter_password": "test", "client_id": "test", "client_name": "test","type":"type","payload":"payload","target":"target"}]}`)
	n.Publish("executions.create", message)

	time.Sleep(500 * time.Millisecond)

	if e := wait(ch); e != nil {
		t.Fatal("Message not received from nats for subscription")
	}
}

func TestInstancesCreateWithInvalidMessage(t *testing.T) {
	os.Setenv("NATS_URI", "nats://localhost:4222")
	os.Setenv("REDIS_ADDR", "localhost:6379")

	n := natsClient()
	r := redisClient()

	processRequest(n, r, "executions.create", "execution.create")

	ch := make(chan bool)
	ch2 := make(chan bool)

	n.Subscribe("execution.create", func(msg *nats.Msg) {
		ch <- true
	})

	n.Subscribe("executions.create.error", func(msg *nats.Msg) {
		ch2 <- true
	})

	message := []byte(`{"service":"service","service_name":"service_name","service_type":"service_type","options":{"user":"user","password":"password"},"service_endpoint":"endpoint","executions":[{"name":"","datacenter":"datacenter","type":"type","payload":"payload","target":"target"}]}`)
	n.Publish("executions.create", message)

	if e := wait(ch); e == nil {
		t.Fatal("Produced a execution.create message when I shouldn't")
	}
	if e := wait(ch2); e != nil {
		t.Fatal("Should produce a executions.create.error message on nats")
	}
}

func TestInstancesCreateWithDifferentMessageType(t *testing.T) {
	os.Setenv("NATS_URI", "nats://localhost:4222")
	os.Setenv("REDIS_ADDR", "localhost:6379")

	n := natsClient()
	r := redisClient()

	processRequest(n, r, "executions.create", "execution.create")

	ch := make(chan bool)

	n.Subscribe("execution.create", func(msg *nats.Msg) {
		ch <- true
	})

	message := []byte(`{"service":"service","service_name":"service_name","service_type":"service_type","options":{"user":"user","password":"password"},"service_endpoint":"endpoint","executions":[{"name":"name","datacenter": "test", "datacenter_name": "test", "datacenter_type": "vcloud", "datacenter_region": "LON-001", "datacenter_username": "test@test", "datacenter_password": "test", "client_id": "test", "client_name": "test", "type":"type","payload":"payload","target":"target"}]}`)
	n.Publish("invalid", message)

	if e := wait(ch); e == nil {
		t.Fatal("Produced a execution.create message when I shouldn't")
	}
}

func TestInstanceCreatedForAMultiRequest(t *testing.T) {
	os.Setenv("NATS_URI", "nats://localhost:4222")
	os.Setenv("REDIS_ADDR", "localhost:6379")

	n := natsClient()
	r := redisClient()

	processResponse(n, r, "execution.create.done", "executions.create.", "execution.create", "completed")
	ch := make(chan bool)

	n.Subscribe("executions.create.done", func(msg *nats.Msg) {
		t.Fatal("Message received from nats does not match")
	})

	original := `{"service":"sss","service_name":"service_name","service_type":"service_type","options":{"user":"user","password":"password"},"service_endpoint":"endpoint","executions":[{"name":"name","datacenter":"datacenter","type":"type","payload":"payload","target":"target"}]}`
	if err := r.Set("GPBExecutions_sss", original, 0).Err(); err != nil {
		log.Println(err)
		t.Fatal("Can't write on redis")
	}
	message := []byte(`{"type":"execution.create.done","service_id":"sss","name":"name", "execution_id":"2", "execution_name": "test", "execution_target": "target", "execution_payload": "payload", "execution_matched_instances": ["test"], "execution_results": {"reports":[{"code": 0, "instance":"test", "stdout":"---", "stderr":""}]}, "execution_status":"success"}`)
	n.Publish("execution.create.done", message)

	if e := wait(ch); e != nil {
		return
	}
}

func TestExecutionsCreatedSingle(t *testing.T) {
	os.Setenv("NATS_URI", "nats://localhost:4222")
	os.Setenv("REDIS_ADDR", "localhost:6379")

	n := natsClient()
	r := redisClient()

	processResponse(n, r, "execution.create.done", "executions.create.", "execution.create", "completed")
	ch := make(chan bool)

	n.Subscribe("executions.create.done", func(m *nats.Msg) {
		event := &ExecutionsCreate{}
		json.Unmarshal(m.Data, event)
		if event.Service == "sss" && event.Status == "completed" && len(event.Executions) == 1 {
			ch <- true
		} else {
			t.Fatal("Message received from nats does not match")
		}
	})

	original := `{"service":"sss","service_name":"service_name","service_type":"service_type","options":{"user":"user","password":"password"},"service_endpoint":"endpoint","executions":[{"name":"name","datacenter":"datacenter","type":"type","payload":"payload","target":"target"}]}`
	if err := r.Set("GPBExecutions_sss", original, 0).Err(); err != nil {
		log.Println(err)
		t.Fatal("Can't write on redis")
	}
	message := []byte(`{"type":"execution.create.done","service_id":"sss","name":"name", "execution_id":"2", "execution_name": "name", "execution_target": "target", "execution_payload": "payload", "execution_matched_instances": ["test"], "execution_results": {"reports":[{"code": 0, "instance":"test", "stdout":"---", "stderr":""}]}, "execution_status":"success"}`)
	n.Publish("execution.create.done", message)

	if e := wait(ch); e != nil {
		t.Fatal("Message not received from nats for subscription")
	}
}

func TestProvisionExecutionsError(t *testing.T) {
	os.Setenv("NATS_URI", "nats://localhost:4222")
	os.Setenv("REDIS_ADDR", "localhost:6379")

	n := natsClient()
	r := redisClient()

	processResponse(n, r, "execution.create.error", "executions.create.", "execution.create", "errored")
	ch := make(chan bool)
	service := "sss"

	n.Subscribe("executions.create.error", func(m *nats.Msg) {
		event := &ExecutionsCreate{}
		json.Unmarshal(m.Data, event)
		if service == event.Service && event.Status == "error" {
			ch <- true
		} else {
			t.Fatal("Message received from nats does not match")
		}
	})

	original := `{"service":"sss","service_name":"service_name","service_type":"service_type","options":{"user":"user","password":"password"},"service_endpoint":"endpoint","executions":[{"name":"name","datacenter":"datacenter","type":"type","payload":"payload","target":"target"}]}`
	if err := r.Set("GPBExecutions_sss", original, 0).Err(); err != nil {
		log.Println(err)
		t.Fatal("Can't write on redis")
	}
	message := []byte(`{"type":"execution.create.error","service_id":"sss","execution_name":"name"}`)
	n.Publish("execution.create.error", message)

	if e := wait(ch); e != nil {
		t.Fatal("Message not received from nats for subscription")
	}
}

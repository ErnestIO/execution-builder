/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package main

import (
	"encoding/json"
	"log"

	"github.com/nats-io/nats"
	"gopkg.in/redis.v3"
)

// Emits a execution.create message to NATS with all necessary info
func startExecution(natsClient *nats.Conn, e ExecutionsCreateExecution, c ExecutionsCreate) {
	subject := "execution.create"
	event := executionEvent{}
	event.load(subject, e, c)
	natsClient.Publish(subject, []byte(event.toJSON()))
}

func processNext(n *nats.Conn, r *redis.Client, subject string, procSubject string, body []byte, status string) (*ExecutionsCreate, bool) {
	event := &executionEvent{}
	json.Unmarshal(body, event)

	message, err := r.Get(event.cacheKey()).Result()
	if err != nil {
		log.Println(err)
	}
	stored := &ExecutionsCreate{}
	json.Unmarshal([]byte(message), stored)
	completed := true
	scheduled := false
	for i := range stored.Executions {
		if stored.Executions[i].Name == event.Name {
			stored.Executions[i].Created = true
			stored.Executions[i].Reports = event.ExecutionResults.Reports
			stored.Executions[i].MatchedInstances = event.ExecutionMatchedInstances
			//stored.Executions[i].Status = event.ExecutionStatus
			stored.Executions[i].Service = event.Service

			if status == "errored" {
				stored.Executions[i].ErrorCode = string(event.Error.Code)
				stored.Executions[i].ErrorMessage = event.Error.Message
				stored.Executions[i].fail()
			} else {
				stored.Executions[i].complete()
			}
		}
		if stored.Executions[i].completed() == false && stored.Executions[i].errored() == false {
			println(stored.Executions[i].Status)
			completed = false
		}
		if stored.Executions[i].toBeProcessed() && scheduled == false {
			scheduled = true
			completed = false
			stored.Executions[i].processing()
			startExecution(n, stored.Executions[i], *stored)
		}
	}
	persistEvent(r, stored)

	return stored, completed
}

func processResponse(n *nats.Conn, r *redis.Client, s string, res string, p string, t string) {
	n.Subscribe(s, func(m *nats.Msg) {
		stored, completed := processNext(n, r, s, p, m.Data, t)
		if completed {
			complete(n, stored, res)
		}
	})
}

func complete(n *nats.Conn, stored *ExecutionsCreate, subject string) {
	if isErrored(stored) == true {
		stored.Status = "error"
		stored.ErrorCode = "0002"
		stored.ErrorMessage = "Some instances could not been successfully processed"
		n.Publish(subject+"error", *stored.toJSON())
	} else {
		stored.Status = "completed"
		stored.ErrorCode = ""
		stored.ErrorMessage = ""
		n.Publish(subject+"done", *stored.toJSON())
	}
}

func isErrored(stored *ExecutionsCreate) bool {
	for _, v := range stored.Executions {
		if v.isErrored() {
			return true
		}
	}
	return false
}

func processRequest(n *nats.Conn, r *redis.Client, subject string, resSubject string) {
	n.Subscribe(subject, func(m *nats.Msg) {
		event := ExecutionsCreate{}
		json.Unmarshal(m.Data, &event)

		persistEvent(r, &event)

		if len(event.Executions) == 0 || event.Status == "completed" {
			event.Status = "completed"
			event.ErrorCode = ""
			event.ErrorMessage = ""
			n.Publish(subject+".done", *event.toJSON())
			return
		}

		for _, execution := range event.Executions {
			if ok, msg := execution.Valid(); ok == false {
				event.Status = "error"
				event.ErrorCode = "0001"
				event.ErrorMessage = msg
				n.Publish(subject+".error", *event.toJSON())
				return
			}
		}

		sw := false
		for i := range event.Executions {
			if event.Executions[i].completed() == false {
				sw = true
				event.Executions[i].processing()
				startExecution(n, event.Executions[i], event)
				if true == event.SequentialProcessing {
					break
				}
			}
		}

		if sw == false {
			event.Status = "completed"
			event.ErrorCode = ""
			event.ErrorMessage = ""
			n.Publish(subject+".done", *event.toJSON())
			return
		}

		persistEvent(r, &event)
	})
}

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package main

import (
	"bytes"
	"encoding/json"
	"log"

	"gopkg.in/redis.v3"
)

type Error struct {
	Code    json.Number `json:"code,Number"`
	Message string      `json:"message"`
}

type executionEvent struct {
	Type             string         `json:"type"`
	Created          bool           `json:"created"`
	Name             string         `json:"execution_name"`
	DatacenterName   string         `json:"datacenter_name"`
	ClientName       string         `json:"client_name"`
	Service          string         `json:"service_id"`
	ServiceName      string         `json:"service_name"`
	ServiceType      string         `json:"service_type"`
	ServiceEndPoint  string         `json:"service_endpoint"`
	ServiceOptions   ServiceOptions `json:"service_options"`
	ExecutionType    string         `json:"execution_type"`
	ExecutionPayload string         `json:"execution_payload"`
	ExecutionTarget  string         `json:"execution_target"`
	ExecutionResults struct {
		Reports []Report `json:"reports,omitempty"`
	} `json:"execution_results,omitempty"`
	ExecutionMatchedInstances []string `json:"execution_matched_instances,omitempty"`
	ExecutionStatus           string   `json:"execution_status,omitempty"`
	Error                     Error    `type:"error"`
}

func (self *executionEvent) load(t string, e ExecutionsCreateExecution, c ExecutionsCreate) {
	self.Service = c.Service
	self.Type = t
	self.Name = e.Name
	self.DatacenterName = e.DatacenterName
	self.ClientName = e.ClientName
	self.ServiceName = c.ServiceName
	self.ServiceType = c.ServiceType
	self.ServiceEndPoint = c.EndPoint
	self.ServiceOptions = c.Options
	self.ExecutionType = e.Type
	self.ExecutionPayload = e.Payload
	self.ExecutionTarget = e.Target
}

func (self *executionEvent) cacheKey() string {
	return composeCacheKey(self.Service)
}

func cacheKey(e *ExecutionsCreate) string {
	return composeCacheKey(e.Service)
}

func (e *executionEvent) toJSON() string {
	message, _ := json.Marshal(e)
	return string(message)
}

func persistEvent(redisClient *redis.Client, event *ExecutionsCreate) {
	if err := redisClient.Set(cacheKey(event), string(*event.ToJSON()), 0).Err(); err != nil {
		log.Println(err)
	}
}

func persistCompletedEvent(redisClient *redis.Client, event *ExecutionsCreate) {
	if err := redisClient.Set(composeCacheKey(event.Service), string(*event.ToJSON()), 0).Err(); err != nil {
		log.Println(err)
	}
}

func composeCacheKey(service string) string {
	var key bytes.Buffer
	key.WriteString("GPBExecutions_")
	key.WriteString(service)

	return key.String()
}

// ServiceOptions ...
type ServiceOptions struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

// Report ...
type Report struct {
	Code     int    `json:"return_code"`
	Instance string `json:"instance"`
	StdErr   string `json:"stderr"`
	StdOut   string `json:"stdout"`
}

// ExecutionsCreateExecution ...
type ExecutionsCreateExecution struct {
	Created            bool     `json:"created"`
	Name               string   `json:"name"`
	Service            string   `json:"service"`
	Type               string   `json:"type"`
	Payload            string   `json:"payload"`
	Target             string   `json:"target"`
	Reports            []Report `json:"reports,omitempty"`
	MatchedInstances   []string `json:"matched_instances,omitempty"`
	ClientName         string   `json:"client_name,omitempty"`
	DatacenterType     string   `json:"datacenter_type,omitempty"`
	DatacenterName     string   `json:"datacenter_name,omitempty"`
	DatacenterUsername string   `json:"datacenter_username,omitempty"`
	DatacenterPassword string   `json:"datacenter_password,omitempty"`
	DatacenterRegion   string   `json:"datacenter_region,omitempty"`
	Status             string   `json:"status"`
	ErrorCode          string   `json:"error_code"`
	ErrorMessage       string   `json:"error_message"`
}

func (r *ExecutionsCreateExecution) fail() {
	r.Status = "errored"
}

func (r *ExecutionsCreateExecution) complete() {
	r.Status = "completed"
}

func (r *ExecutionsCreateExecution) processing() {
	r.Status = "processed"
}

func (r *ExecutionsCreateExecution) errored() bool {
	return r.Status == "errored"
}

func (r *ExecutionsCreateExecution) completed() bool {
	return r.Status == "completed"
}

func (r *ExecutionsCreateExecution) isProcessed() bool {
	return r.Status == "processed"
}

func (r *ExecutionsCreateExecution) isErrored() bool {
	return r.Status == "errored"
}

func (r *ExecutionsCreateExecution) toBeProcessed() bool {
	return r.Status != "processed" && r.Status != "completed" && r.Status != "errored"
}

// ExecutionsCreate ...
type ExecutionsCreate struct {
	Service              string                      `json:"service"`
	ServiceName          string                      `json:"service_name"`
	ServiceType          string                      `json:"service_type"`
	Executions           []ExecutionsCreateExecution `json:"executions"`
	Options              ServiceOptions              `json:"options"`
	EndPoint             string                      `json:"service_endpoint"`
	Status               string                      `json:"status"`
	ErrorCode            string                      `json:"error_code"`
	ErrorMessage         string                      `json:"error_message"`
	SequentialProcessing bool                        `json:"sequential_processing"`
}

// ToJSON ...
func (executions *ExecutionsCreate) toJSON() *[]byte {
	jsn, err := json.Marshal(executions)
	if err != nil {
		log.Println(err)
	}
	return &jsn
}

// Valid ...
func (execution *ExecutionsCreateExecution) Valid() (bool, string) {
	if execution.Name == "" {
		return false, "Execution name is empty"
	}
	return true, ""
}

// ToJSON ...
func (execution *ExecutionsCreateExecution) ToJSON() *[]byte {
	jsn, err := json.Marshal(execution)
	if err != nil {
		log.Println(err)
	}
	return &jsn
}

// ToJSON ...
func (execution *ExecutionsCreate) ToJSON() *[]byte {
	jsn, err := json.Marshal(execution)
	if err != nil {
		log.Println(err)
	}
	return &jsn
}

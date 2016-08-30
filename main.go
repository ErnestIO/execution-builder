/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package main

import (
	"os"
	"runtime"

	l "github.com/ernestio/builder-library"
)

var s l.Scheduler

func main() {
	s.Setup(os.Getenv("NATS_URI"))

	// Process requests
	s.ProcessRequest("executions.create", "execution.create")

	// Process resulting success
	s.ProcessSuccessResponse("execution.create.done", "execution.create", "executions.create.done")

	// Process resulting errors
	s.ProcessFailedResponse("execution.create.error", "executions.create.error")

	runtime.Goexit()
}

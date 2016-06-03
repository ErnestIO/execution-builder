/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package main

import "runtime"

func main() {
	n := natsClient()
	r := redisClient()

	// Process requests
	processRequest(n, r, "executions.create", "execution.create")

	// Process resulting success
	processResponse(n, r, "execution.create.done", "executions.create.", "execution.create", "completed")

	// Process resulting errors
	processResponse(n, r, "execution.create.error", "executions.create.", "execution.create", "errored")

	runtime.Goexit()
}

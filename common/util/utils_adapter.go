/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
Modifications Copyright National Payments Corporation of India
*/
package util

import (
	"math"
	"time"

	"github.com/npci/drunix/common/flogging"
)

var logger = flogging.MustGetLogger("common.util")

/*
DRUNIX

	ExponentialBackoffRetry attempts to execute the given operation function with an exponential backoff retry mechanism.
	If the operation fails, it will retry up to maxRetries times, with an initial delay that doubles after each retry.

	Parameters:
	- operation: The function to be executed, which returns an error if it fails.
	- maxRetries: The maximum number of retry attempts.
	- initialDelay: The initial delay duration before the first retry, which will double after each subsequent retry.

	Returns:
	- error: The error returned by the operation function if all retry attempts fail, or nil if the operation succeeds.
*/
func ExponentialBackoffRetry(operation func() error, maxRetries int, initialDelay time.Duration) error {
	err := operation()
	if err == nil {
		return nil
	}
	for i := 0; i < maxRetries; i++ {
		delay := initialDelay * time.Duration(math.Pow(2, float64(i)))
		logger.Errorf("retrying after %v (%d/%d) for error : %v\n", delay, i+1, maxRetries, err)
		time.Sleep(delay)
		err = operation()
		if err == nil {
			return nil
		}
	}
	return err
}

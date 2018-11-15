/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.async;

import java.time.Duration;

import static io.datakernel.util.Preconditions.checkArgument;
import static java.lang.Math.*;

@FunctionalInterface
public interface RetryPolicy {
	long nextRetryTimestamp(long now, Throwable lastError, int retryCount, long firstRetryTimestamp);

	static RetryPolicy noRetry() {
		return (now, lastError, retryCount, retryTimestamp) -> 0;
	}

	static RetryPolicy immediateRetry() {
		return (now, lastError, retryCount, retryTimestamp) -> now;
	}

	static RetryPolicy fixedDelay(long delay) {
		return (now, lastError, retryCount, retryTimestamp) -> now + delay;
	}

	static RetryPolicy exponentialBackoff(long initialDelay, long maxDelay, double exponent) {
		checkArgument(maxDelay > initialDelay && exponent > 1.0,
				"Max delay should be greater than initial delay and exponent should be greater than 1.0");
		int maxRetryCount = (int) ceil(log((double) maxDelay / initialDelay) / log(exponent));
		return (now, lastError, retryCount, retryTimestamp) -> now + (
				retryCount > maxRetryCount ?
						maxDelay :
						min(maxDelay, (long) (initialDelay * pow(exponent, retryCount))));
	}

	static RetryPolicy exponentialBackoff(long initialDelay, long maxDelay) {
		return exponentialBackoff(initialDelay, maxDelay, 2.0);
	}

	default RetryPolicy withMaxTotalRetryCount(int maxRetryCount) {
		return (now, lastError, retryCount, errorTimestamp) ->
				retryCount < maxRetryCount ? nextRetryTimestamp(now, lastError, retryCount, errorTimestamp) : 0;
	}

	default RetryPolicy withMaxTotalRetryTimeout(Duration maxRetryTimeout) {
		return (now, lastError, retryCount, retryTimestamp) -> {
			long nextRetryTimestamp = nextRetryTimestamp(now, lastError, retryCount, retryTimestamp);
			return nextRetryTimestamp - retryTimestamp < maxRetryTimeout.toMillis() ? nextRetryTimestamp : 0;
		};
	}

}

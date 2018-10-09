package io.datakernel.async;

import java.time.Duration;

import static io.datakernel.util.Preconditions.checkArgument;
import static java.lang.Math.*;

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
		checkArgument(maxDelay > initialDelay && exponent > 1.0);
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

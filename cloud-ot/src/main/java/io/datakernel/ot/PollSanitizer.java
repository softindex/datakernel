package io.datakernel.ot;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Objects;

import static io.datakernel.promise.RetryPolicy.exponentialBackoff;

public final class PollSanitizer<T> implements AsyncSupplier<T> {
	public static final Duration DEFAULT_YIELD_INTERVAL = Duration.ofMillis(1000L);

	private Duration yieldInterval = DEFAULT_YIELD_INTERVAL;

	private final AsyncSupplier<T> poll;

	@Nullable
	private T lastValue;

	private PollSanitizer(AsyncSupplier<T> poll) {
		this.poll = poll;
	}

	public static <T> PollSanitizer<T> create(AsyncSupplier<T> poll) {
		return new PollSanitizer<>(poll);
	}

	public PollSanitizer<T> withYieldInterval(Duration yieldInterval) {
		this.yieldInterval = yieldInterval;
		return this;
	}

	@Override
	public Promise<T> get() {
		return Promises.retry(poll,
				(value, e) -> {
					if (e != null) return true;
					if (Objects.equals(value, lastValue)) {
						return false;
					} else {
						this.lastValue = value;
						return true;
					}
				},
				exponentialBackoff(Duration.ofMillis(1), yieldInterval));
	}
}

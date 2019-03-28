package io.datakernel.ot;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Objects;

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
		return Promises.until(poll,
				value -> {
					if (!Objects.equals(value, lastValue)) {
						this.lastValue = value;
						return Promise.of(true);
					} else {
						return Promises.delay(Promise.of(false), yieldInterval);
					}
				});
	}
}

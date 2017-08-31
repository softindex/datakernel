package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.AsyncTimeoutException;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;

public class AsyncFunctions {
	public static final AsyncTimeoutException TIMEOUT_EXCEPTION = new AsyncTimeoutException();

	private AsyncFunctions() {
	}

	private static final class DoneState {
		boolean done;
	}

	public static <I, O> AsyncFunction<I, O> timeout(final Eventloop eventloop, final long timestamp, final AsyncFunction<I, O> function) {
		return input -> {
			final DoneState state = new DoneState();
			final SettableStage<O> stage = SettableStage.create();

			function.apply(input).whenComplete((o, throwable) -> {
				if (!state.done) state.done = true;
				AsyncCallbacks.forwardTo(stage, o, throwable);
			});
			if (!state.done) {
				eventloop.schedule(timestamp, () -> {
					if (!state.done) {
						state.done = true;
						stage.setError(TIMEOUT_EXCEPTION);
					}
				});
			}

			return stage;
		};
	}

	public static <I, T, O> AsyncFunction<I, O> compose(AsyncFunction<T, O> g, AsyncFunction<I, T> f) {
		return pipeline(f, g);
	}

	public static <I, T, O> AsyncFunction<I, O> pipeline(final AsyncFunction<I, T> f, final AsyncFunction<T, O> g) {
		return input -> f.apply(input).thenCompose(g::apply);
	}

	public static <I, O> AsyncFunction<I, O> pipeline(AsyncFunction<?, ?>... functions) {
		return pipeline(Arrays.asList(functions));
	}

	@SuppressWarnings("unchecked")
	public static <I, O> AsyncFunction<I, O> pipeline(final Iterable<AsyncFunction<?, ?>> functions) {
		return new AsyncFunction<I, O>() {
			@Override
			public CompletionStage<O> apply(I input) {
				final SettableStage<O> stage = SettableStage.create();
				next(input, (Iterator) functions.iterator(), stage);
				return stage;
			}

			private void next(Object item, final Iterator<AsyncFunction<Object, Object>> iterator, final SettableStage<O> stage) {
				if (iterator.hasNext()) {
					AsyncFunction<Object, Object> next = iterator.next();
					next.apply(item).whenComplete((o, throwable) -> {
						if (throwable == null) {
							next(o, iterator, stage);
						} else {
							stage.setError(throwable);
						}
					});
				} else {
					stage.setResult((O) item);
				}
			}
		};
	}
}

package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.util.Function;

import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@SuppressWarnings("WeakerAccess")
public class AsyncCallbacks {
	private AsyncCallbacks() {
	}

	public static <T> ResultCallback<T> resultToStage(SettableStage<T> stage) {
		return new ResultCallback<T>() {
			@Override
			protected void onResult(T result) {
				stage.set(result);
			}

			@Override
			protected void onException(Exception e) {
				stage.setException(e);
			}
		};
	}

	public static CompletionCallback completionToStage(SettableStage<Void> stage) {
		return new CompletionCallback() {
			@Override
			protected void onComplete() {
				stage.set(null);
			}

			@Override
			protected void onException(Exception e) {
				stage.setException(e);
			}
		};
	}

	public static BiConsumer<SocketChannel, Throwable> forwardTo(ConnectCallback callback) {
		return new BiConsumer<SocketChannel, Throwable>() {
			@Override
			public void accept(SocketChannel socketChannel, Throwable throwable) {
				if (throwable == null) {
					callback.setConnect(socketChannel);
				} else {
					callback.setException(AsyncCallbacks.throwableToException(throwable));
				}
			}
		};
	}

	public static <T> BiConsumer<T, Throwable> forwardTo(CompletionCallback callback) {
		return (o, throwable) -> forwardTo(callback, throwable);
	}

	public static void forwardTo(CompletionCallback callback, Throwable throwable) {
		if (throwable == null)
			callback.setComplete();
		else
			callback.setException(throwableToException(throwable));
	}

	public static <T> BiConsumer<T, Throwable> forwardTo(ResultCallback<T> callback) {
		return (o, throwable) -> {
			if (throwable == null)
				callback.setResult(o);
			else
				callback.setException(throwableToException(throwable));
		};
	}

	public static Exception throwableToException(Throwable throwable) {
		return throwable instanceof Exception ? (Exception) throwable : new RuntimeException(throwable);
	}

	public static <T> BiConsumer<T, Throwable> forwardTo(SettableStage<T> stage) {
		return (o, throwable) -> forwardTo(stage, o, throwable);
	}

	public static <T> void forwardTo(SettableStage<T> stage, T o, Throwable throwable) {
		if (throwable == null) {
			stage.set(o);
		} else {
			stage.setException(throwable);
		}
	}


	public static <T> ResultCallback<T> ignoreResult() {
		return IgnoreResultCallback.create();
	}

	public static CompletionCallback ignoreCompletion() {
		return IgnoreCompletionCallback.create();
	}

	public static <T> ResultCallback<T> assertResult() {
		return new AssertingResultCallback<>();
	}

	public static CompletionCallback assertCompletion() {
		return new AssertingCompletionCallback();
	}

	public static <T> BiConsumer<T, ? super Throwable> assertBiConsumer(Consumer<T> consumer) {
		return (BiConsumer<T, Throwable>) (t, throwable) -> {
			if (throwable != null) throw new AssertionError("Fatal error in bi consumer", throwable);
			consumer.accept(t);
		};
	}

	public static <I, O> ResultCallback<I> transformTo(final ResultCallback<O> callback, final Function<I, O> function) {
		return new ForwardingResultCallback<I>(callback) {
			@Override
			protected void onResult(I result) {
				callback.setResult(function.apply(result));
			}
		};
	}

	public static <T> CompletionCallback toResultCallback(final ResultCallback<T> callback, final T value) {
		return new ForwardingCompletionCallback(callback) {
			@Override
			protected void onComplete() {
				callback.setResult(value);
			}
		};
	}

	public static <T> ResultCallback<T> toCompletionCallback(final CompletionCallback callback) {
		return new ForwardingResultCallback<T>(callback) {
			@Override
			protected void onResult(T ignored) {
				callback.setComplete();
			}
		};
	}

	public static CompletionCallback toCompletionCallbacks(final Collection<CompletionCallback> callbacks) {
		return new CompletionCallback() {
			@Override
			protected void onComplete() {
				for (CompletionCallback callback : callbacks) {
					callback.setComplete();
				}
			}

			@Override
			protected void onException(Exception e) {
				for (CompletionCallback callback : callbacks) {
					callback.setException(e);
				}
			}
		};
	}

	public static CompletionCallback toCompletionCallbacks(final CompletionCallback... callbacks) {
		return toCompletionCallbacks(Arrays.asList(callbacks));
	}

	public static <T> ResultCallback<T> toResultCallbacks(final Collection<? extends ResultCallback<T>> callbacks) {
		return new ResultCallback<T>() {
			@Override
			protected void onResult(T result) {
				for (ResultCallback<T> callback : callbacks) {
					callback.setResult(result);
				}
			}

			@Override
			protected void onException(Exception e) {
				for (ResultCallback<T> callback : callbacks) {
					callback.setException(e);
				}
			}
		};
	}

	@SafeVarargs
	public static <T> ResultCallback<T> toResultCallbacks(final ResultCallback<T>... callbacks) {
		return new ResultCallback<T>() {
			@Override
			protected void onResult(T result) {
				for (ResultCallback<T> callback : callbacks) {
					callback.setResult(result);
				}
			}

			@Override
			protected void onException(Exception e) {
				for (ResultCallback<T> callback : callbacks) {
					callback.setException(e);
				}
			}
		};
	}

	public static <T> ResultCallback<T> postTo(final Eventloop eventloop, final ResultCallback<T> callback) {
		return new ResultCallback<T>() {
			@Override
			protected void onResult(T result) {
				callback.postResult(eventloop, result);
			}

			@Override
			protected void onException(Exception e) {
				callback.postException(eventloop, e);
			}
		};
	}

	public static <T> ResultCallback<T> postTo(final ResultCallback<T> callback) {
		return new ResultCallback<T>() {
			@Override
			protected void onResult(T result) {
				callback.postResult(result);
			}

			@Override
			protected void onException(Exception e) {
				callback.postException(e);
			}
		};
	}

	public static CompletionCallback postTo(final Eventloop eventloop, final CompletionCallback callback) {
		return new CompletionCallback() {
			@Override
			protected void onComplete() {
				callback.postComplete(eventloop);
			}

			@Override
			protected void onException(Exception e) {
				callback.postException(eventloop, e);
			}
		};
	}

	public static CompletionCallback postTo(final CompletionCallback callback) {
		return new CompletionCallback() {
			@Override
			protected void onComplete() {
				callback.postComplete();
			}

			@Override
			protected void onException(Exception e) {
				callback.postException(e);
			}
		};
	}

	public static <T> ResultCallback<T> postToAnotherEventloop(final Eventloop eventloop, final ResultCallback<T> callback) {
		return new ResultCallback<T>() {
			@Override
			protected void onResult(final T result) {
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						callback.setResult(result);
					}
				});
			}

			@Override
			protected void onException(final Exception e) {
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						callback.setException(e);
					}
				});
			}
		};
	}

	public static CompletionCallback postToAnotherEventloop(final Eventloop anotherEventloop, final CompletionCallback callback) {
		return new CompletionCallback() {
			@Override
			protected void onComplete() {
				anotherEventloop.execute(new Runnable() {
					@Override
					public void run() {
						callback.setComplete();
					}
				});
			}

			@Override
			protected void onException(final Exception e) {
				anotherEventloop.execute(new Runnable() {
					@Override
					public void run() {
						callback.setException(e);
					}
				});
			}
		};
	}
}

/*
 * Copyright (C) 2015 SoftIndex LLC.
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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopServer;
import io.datakernel.eventloop.EventloopService;
import org.slf4j.Logger;

import java.io.IOException;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Static utility methods pertaining to {@link ResultCallback}, {@link CompletionCallback} and working with
 * {@link EventloopServer} and {@link EventloopService}.
 */
public final class AsyncCallbacks {
	private final Logger logger = getLogger(this.getClass());

	private AsyncCallbacks() {}

	/**
	 * Returns CompletionCallback, which no reaction on its callings.
	 */
	public static CompletionCallback ignoreCompletionCallback() {
		return new CompletionCallback() {
			@Override
			public void onComplete() {

			}

			@Override
			public void onException(Exception exception) {

			}
		};
	}

	/**
	 * Returns ResultCallback, which no reaction on its callings.
	 */
	@SuppressWarnings("unchecked")
	public static <T> ResultCallback<T> ignoreResultCallback() {
		return new ResultCallback<T>() {
			@Override
			public void onResult(Object result) {
			}

			@Override
			public void onException(Exception exception) {
			}
		};
	}

	private static final AsyncCancellable NOT_CANCELLABLE = new AsyncCancellable() {
		@Override
		public void cancel() {
			// Do nothing
		}
	};

	public static AsyncCancellable notCancellable() {
		return NOT_CANCELLABLE;
	}

	public static <T> void postResult(Eventloop eventloop, final ResultCallback<T> callback, final T result) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.setResult(result);
			}
		});
	}

	public static void postException(Eventloop eventloop, final ExceptionCallback callback, final Exception e) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.setException(e);
			}
		});
	}

	public static void postCompletion(Eventloop eventloop, final CompletionCallback callback) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.setComplete();
			}
		});
	}

	public static <T> void setResultIn(Eventloop eventloop, final ResultCallback<T> callback, final T result) {
		eventloop.execute(new Runnable() {
			@Override
			public void run() {
				callback.setResult(result);
			}
		});
	}

	public static void setExceptionIn(Eventloop eventloop, final ExceptionCallback callback, final Exception e) {
		eventloop.execute(new Runnable() {
			@Override
			public void run() {
				callback.setException(e);
			}
		});
	}

	public static void setCompleteIn(Eventloop eventloop, final CompletionCallback callback) {
		eventloop.execute(new Runnable() {
			@Override
			public void run() {
				callback.setComplete();
			}
		});
	}

	public static final class WaitAllHandler {
		private final int minCompleted;
		private final int totalCount;
		private final CompletionCallback callback;

		private int completed = 0;
		private int exceptions = 0;
		private Exception lastException;

		private WaitAllHandler(int minCompleted, int totalCount, CompletionCallback callback) {
			this.minCompleted = minCompleted;
			this.totalCount = totalCount;
			this.callback = callback;
		}

		public CompletionCallback getCallback() {
			return new CompletionCallback() {
				@Override
				protected void onComplete() {
					++completed;
					completeResult();
				}

				@Override
				protected void onException(Exception exception) {
					++exceptions;
					lastException = exception;
					completeResult();
				}
			};
		}

		private void completeResult() {
			if ((exceptions + completed) == totalCount) {
				if (completed >= minCompleted) {
					callback.setComplete();
				} else {
					callback.setException(lastException);
				}
			}
		}
	}

	public static WaitAllHandler waitAll(int count, CompletionCallback callback) {
		if (count == 0)
			callback.setComplete();

		return new WaitAllHandler(count, count, callback);
	}

	public static WaitAllHandler waitAll(int minCompleted, int totalCount, CompletionCallback callback) {
		if (totalCount == 0)
			callback.setComplete();

		return new WaitAllHandler(minCompleted, totalCount, callback);
	}

	public static CompletionCallbackFuture listenFuture(final EventloopServer server) {
		final CompletionCallbackFuture future = CompletionCallbackFuture.create();
		server.getEventloop().execute(new Runnable() {
			@Override
			public void run() {
				try {
					server.listen();
					future.setComplete();
				} catch (IOException e) {
					future.setException(e);
				}
			}
		});
		return future;
	}

	public static CompletionCallbackFuture closeFuture(final EventloopServer server) {
		final CompletionCallbackFuture future = CompletionCallbackFuture.create();
		server.getEventloop().execute(new Runnable() {
			@Override
			public void run() {
				server.close(new CompletionCallback() {
					@Override
					protected void onComplete() {
						future.setComplete();
					}

					@Override
					protected void onException(Exception e) {
						future.setException(e);
					}
				});
			}
		});
		return future;
	}

	public static CompletionCallbackFuture startFuture(final EventloopService eventloopService) {
		final CompletionCallbackFuture future = CompletionCallbackFuture.create();
		eventloopService.getEventloop().execute(new Runnable() {
			@Override
			public void run() {
				eventloopService.start(future);
			}
		});
		return future;
	}

	public static CompletionCallbackFuture stopFuture(final EventloopService eventloopService) {
		final CompletionCallbackFuture future = CompletionCallbackFuture.create();
		eventloopService.getEventloop().execute(new Runnable() {
			@Override
			public void run() {
				eventloopService.stop(future);
			}
		});
		return future;
	}

	/**
	 * Returns {@link ResultCallback} which forwards {@code setResult()} or {@code setException()} calls
	 * to specified eventloop
	 *
	 * @param eventloop {@link Eventloop} to which calls will be forwarded
	 * @param callback  {@link ResultCallback}
	 * @param <T>
	 * @return {@link ResultCallback} which forwards {@code onResult()} or {@code onException()} calls
	 * to specified eventloop
	 */
	public static <T> ResultCallback<T> concurrentResultCallback(final Eventloop eventloop,
	                                                             final ResultCallback<T> callback) {
		return new ResultCallback<T>() {
			@Override
			public void onResult(final T result) {
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						callback.setResult(result);
					}
				});
			}

			@Override
			public void onException(final Exception exception) {
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						callback.setException(exception);
					}
				});
			}
		};
	}

	/**
	 * Returns {@link CompletionCallback} which forwards {@code setComplete()} or {@code setException()} calls
	 * to specified eventloop
	 *
	 * @param eventloop {@link Eventloop} to which calls will be forwarded
	 * @param callback  {@link CompletionCallback}
	 * @return {@link CompletionCallback} which forwards {@code onComplete()} or {@code onException()} calls
	 * to specified eventloop
	 */
	public static CompletionCallback concurrentCompletionCallback(final Eventloop eventloop,
	                                                              final CompletionCallback callback) {
		return new CompletionCallback() {
			@Override
			public void onComplete() {
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						callback.setComplete();
					}
				});
			}

			@Override
			public void onException(final Exception exception) {
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						callback.setException(exception);
					}
				});
			}
		};
	}
}

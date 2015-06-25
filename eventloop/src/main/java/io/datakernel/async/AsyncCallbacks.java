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

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioServer;
import io.datakernel.eventloop.NioService;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Static utility methods pertaining to {@link ResultCallback}, {@link CompletionCallback} and working with
 * {@link NioServer} and {@link NioService} with monitoring used {@link ListenableFuture} .
 */
public class AsyncCallbacks {
	private static final Logger logger = getLogger(AsyncCallbacks.class);

	private static final CompletionCallback IGNORE_COMPLETION_CALLBACK = new CompletionCallback() {
		@Override
		public void onComplete() {

		}

		@Override
		public void onException(Exception exception) {

		}
	};

	/**
	 * Returns CompletionCallback, which no reaction on its callings.
	 */
	public static CompletionCallback ignoreCompletionCallback() {
		return IGNORE_COMPLETION_CALLBACK;
	}

	private static final ResultCallback<Object> IGNORE_RESULT_CALLBACK = new ResultCallback<Object>() {
		@Override
		public void onResult(Object result) {
		}

		@Override
		public void onException(Exception exception) {
		}
	};

	/**
	 * Returns ResultCallback, which no reaction on its callings.
	 */
	@SuppressWarnings("unchecked")
	public static <T> ResultCallback<T> ignoreResultCallback() {
		return (ResultCallback<T>) IGNORE_RESULT_CALLBACK;
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

	/**
	 * Calls onResult() from ResultCallback from arguments in event loop from other thread.
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 * @param result    the result with which ResultCallback will be called.
	 * @param <T>       type of result
	 */
	public static <T> void postResultConcurrently(Eventloop eventloop, final ResultCallback<T> callback, final T result) {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				callback.onResult(result);
			}
		});
	}

	/**
	 * Calls onException() from ResultCallback from arguments in event loop from other thread.
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 * @param e         the exception with which ResultCallback will be called.
	 */
	public static void postExceptionConcurrently(Eventloop eventloop, final ResultCallback<?> callback, final Exception e) {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				callback.onException(e);
			}
		});
	}

	/**
	 * Calls onComplete() from CompletionCallback from arguments in event loop from other thread.
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 */
	public static void postCompletionConcurrently(Eventloop eventloop, final CompletionCallback callback) {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				callback.onComplete();
			}
		});
	}

	/**
	 * Calls onException() from CompletionCallback from arguments in event loop from other thread.
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 * @param e         the exception with which CompletionCallback will be called.
	 */
	public static void postExceptionConcurrently(Eventloop eventloop, final CompletionCallback callback, final Exception e) {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				callback.onException(e);
			}
		});
	}

	/**
	 * Calls onNext() from IteratorCallback from arguments in event loop from other thread
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 * @param next      the element with which IteratorCallback will be called.
	 * @param <T>       type of elements in iterator
	 */
	public static <T> void postNextConcurrently(Eventloop eventloop, final IteratorCallback<T> callback, final T next) {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				callback.onNext(next);
			}
		});
	}

	/**
	 * Calls onEnd() from IteratorCallback from arguments in event loop from other thread
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 * @param <T>       type of elements in iterator
	 */
	public static <T> void postEndConcurrently(Eventloop eventloop, final IteratorCallback<T> callback) {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				callback.onEnd();
			}
		});
	}

	/**
	 * Calls onException() from IteratorCallback from arguments in event loop from other thread
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 * @param e         the exception with which IteratorCallback will be called.
	 * @param <T>       type of elements in iterator
	 */
	public static <T> void postExceptionConcurrently(Eventloop eventloop, final IteratorCallback<T> callback, final Exception e) {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				callback.onException(e);
			}
		});
	}

	public static void notifyOnCancel(ExceptionCallback callback, AsyncCancellableStatus.CancelNotifier cancelNotifier) {
		if (callback instanceof AsyncCancellableStatus) {
			((AsyncCancellableStatus) callback).notifyOnCancel(cancelNotifier);
		}
	}

	/**
	 * Checks if the action was cancelled
	 *
	 * @param callback callback for checking
	 * @return true if was, false else
	 */
	public static boolean isCancelled(ExceptionCallback callback) {
		if (callback instanceof AsyncCancellableStatus) {
			return ((AsyncCancellableStatus) callback).isCancelled();
		}
		return false;
	}

	/**
	 * Calls onCancel() and cancels  callback.
	 */
	public static void cancel(ExceptionCallback callback) {
		if (callback instanceof AsyncCancellable) {
			((AsyncCancellable) callback).cancel();
		}
	}

	/**
	 * In new thread calls callable and receives result. Then in Eventloop's thread processes it with
	 * callback's onResult() or onException() and closes callback.
	 *
	 * @param eventloop             in its thread it will be called callback
	 * @param executor              executor for running in new thread
	 * @param mayInterruptIfRunning flag that mean thread in which callable ran could be closed
	 * @param callable              instance which returns result
	 * @param callback              callback for handling result
	 * @param <T>                   type of result
	 */
	public static <T> void callConcurrently(final Eventloop eventloop, ExecutorService executor,
	                                        final boolean mayInterruptIfRunning, final Callable<T> callable,
	                                        final ResultCallback<T> callback) {
		final Eventloop.ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
		final Future<?> future = executor.submit(new Runnable() {
			@Override
			public void run() {
				try {
					final T result = callable.call();
					eventloop.postConcurrently(new Runnable() {
						@Override
						public void run() {
							callback.onResult(result);
						}
					});
				} catch (final Exception e) {
					logger.error("callConcurrently error", e);
					eventloop.postConcurrently(new Runnable() {
						@Override
						public void run() {
							callback.onException(e);
						}
					});
				}
				tracker.complete();
			}
		});
		notifyOnCancel(callback, new AsyncCancellableStatus.CancelNotifier() {
			@Override
			public void onCancel() {
				future.cancel(mayInterruptIfRunning);
			}
		});
	}

	/**
	 * In new thread applies function for value and receives result. Then in Eventloop's thread
	 * processes it with callback's onResult() or onException() and closes callback.
	 *
	 * @param eventloop             in its thread it will be called callback
	 * @param executor              executor for running in new thread
	 * @param mayInterruptIfRunning flag that mean thread in which callable ran could be closed
	 * @param function              function for receiving result
	 * @param value                 value for applying function
	 * @param callback              callback for handling result
	 * @param <I>                   type of value
	 * @param <O>                   type of result
	 */
	public static <I, O> void applyConcurrently(final Eventloop eventloop, ExecutorService executor,
	                                            final boolean mayInterruptIfRunning,
	                                            final Function<I, O> function, final I value,
	                                            final ResultCallback<O> callback) {
		final Eventloop.ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
		final Future<?> future = executor.submit(new Runnable() {
			@Override
			public void run() {
				try {
					final O result = function.apply(value);
					eventloop.postConcurrently(new Runnable() {
						@Override
						public void run() {
							callback.onResult(result);
						}
					});
				} catch (final Exception e) {
					logger.error("applyConcurrently error", e);
					eventloop.postConcurrently(new Runnable() {
						@Override
						public void run() {
							callback.onException(e);
						}
					});
				}
				tracker.complete();
			}
		});
		notifyOnCancel(callback, new AsyncCancellableStatus.CancelNotifier() {
			@Override
			public void onCancel() {
				future.cancel(mayInterruptIfRunning);
			}
		});
	}

	/**
	 * Sets value to Settable from new thread. Then in Eventloop's thread
	 * processes it with callback's onComplete() or onException() and closes callback.
	 *
	 * @param eventloop             in its thread it will be called callback
	 * @param executor              executor for running in new thread
	 * @param mayInterruptIfRunning flag that mean thread in which callable ran could be closed
	 * @param settable              settable for setting
	 * @param value                 value for setting
	 * @param callback              callback for handling complete
	 * @param <T>                   type of value
	 */
	public static <T> void setConcurrently(final Eventloop eventloop, ExecutorService executor,
	                                       final boolean mayInterruptIfRunning,
	                                       final Settable<T> settable, final T value,
	                                       final CompletionCallback callback) {
		final Eventloop.ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
		final Future<?> future = executor.submit(new Runnable() {
			@Override
			public void run() {
				try {
					settable.set(value);
					eventloop.postConcurrently(new Runnable() {
						@Override
						public void run() {
							callback.onComplete();
						}
					});
				} catch (final Exception e) {
					logger.error("setConcurrently error", e);
					eventloop.postConcurrently(new Runnable() {
						@Override
						public void run() {
							callback.onException(e);
						}
					});
				}
				tracker.complete();
			}
		});
		notifyOnCancel(callback, new AsyncCancellableStatus.CancelNotifier() {
			@Override
			public void onCancel() {
				future.cancel(mayInterruptIfRunning);
			}
		});
	}

	/**
	 * Runs runnable in new thread. Then in Eventloop's thread processes it with callback's
	 * onComplete() or onException() and closes callback.
	 *
	 * @param eventloop             in its thread it will be called callback
	 * @param executor              executor for running in new thread
	 * @param mayInterruptIfRunning flag that mean thread in which callable ran could be closed
	 * @param runnable              runnable for running
	 * @param callback              callback for handling complete
	 */
	public static void runConcurrently(final Eventloop eventloop, ExecutorService executor,
	                                   final boolean mayInterruptIfRunning, final Runnable runnable,
	                                   final CompletionCallback callback) {
		final Eventloop.ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
		final Future<?> future = executor.submit(new Runnable() {
			@Override
			public void run() {
				try {
					runnable.run();
					eventloop.postConcurrently(new Runnable() {
						@Override
						public void run() {
							callback.onComplete();
						}
					});
				} catch (final Exception e) {
					logger.error("runConcurrently error", e);
					eventloop.postConcurrently(new Runnable() {
						@Override
						public void run() {
							callback.onException(e);
						}
					});
				}
				tracker.complete();
			}
		});
		notifyOnCancel(callback, new AsyncCancellableStatus.CancelNotifier() {
			@Override
			public void onCancel() {
				future.cancel(mayInterruptIfRunning);
			}
		});

	}

	/**
	 * Returns new  AsyncTask which contains all tasks from argument and executes them successively
	 * through callback after calling execute()
	 *
	 * @param tasks asynchronous tasks for non-blocking running
	 */
	public static AsyncTask sequence(final AsyncTask... tasks) {
		return new AsyncTask() {
			@Override
			public void execute(final CompletionCallback callback) {
				if (tasks.length == 0) {
					callback.onComplete();
				} else {
					CompletionCallback internalCallback = new ForwardingCompletionCallback(callback) {
						int n = 1;

						@Override
						public void onComplete() {
							if (n == tasks.length) {
								callback.onComplete();
							} else {
								AsyncTask task = tasks[n++];
								task.execute(this);
							}
						}
					};

					AsyncTask task = tasks[0];
					task.execute(internalCallback);
				}
			}
		};
	}

	/**
	 * Returns new  AsyncTask which contains two tasks from argument and executes them successively
	 * through callback after calling execute()
	 */
	public static AsyncTask sequence(AsyncTask task1, AsyncTask task2) {
		return sequence(new AsyncTask[]{task1, task2});
	}

	/**
	 * Returns new  AsyncTask which contains tasks from argument and executes them parallel
	 */
	public static AsyncTask parallel(final AsyncTask... tasks) {
		return new AsyncTask() {
			@Override
			public void execute(final CompletionCallback callback) {
				if (tasks.length == 0) {
					callback.onComplete();
				} else {
					CompletionCallback internalCallback = new ForwardingCompletionCallback(callback) {
						int n = tasks.length;

						@Override
						public void onComplete() {
							if (--n == 0) {
								callback.onComplete();
							}
						}

						@Override
						public void onException(Exception exception) {
							if (n > 0) {
								n = 0;
								callback.onException(exception);
							}
						}
					};

					for (AsyncTask task : tasks) {
						task.execute(internalCallback);
					}
				}
			}
		};
	}

	/**
	 * Returns  AsyncGetter which parallel processed results from getters from argument
	 *
	 * @param returnOnFirstException flag that means that on first throwing exception in getters
	 *                               method should returns AsyncGetter
	 */
	public static AsyncGetter<Object[]> parallel(final boolean returnOnFirstException, final AsyncGetter<?>... getters) {
		return new AsyncGetter<Object[]>() {
			class Holder {
				int n = 0;
				Exception[] exceptions;
			}

			@SuppressWarnings("unchecked")
			@Override
			public void get(final ResultCallback<Object[]> callback) {
				final Object[] results = new Object[getters.length];
				if (getters.length == 0) {
					callback.onResult(results);
				} else {
					final Holder holder = new Holder();
					holder.n = getters.length;
					for (int i = 0; i < getters.length; i++) {
						AsyncGetter<Object> getter = (AsyncGetter<Object>) getters[i];

						final int finalI = i;
						getter.get(new ForwardingResultCallback<Object>(callback) {
							private void checkCompleteResult() {
								if (--holder.n == 0) {
									if (holder.exceptions == null)
										callback.onResult(results);
									else
										callback.onException(new ParallelExecutionException(results, holder.exceptions));
								}
							}

							@Override
							public void onResult(Object result) {
								results[finalI] = result;
								checkCompleteResult();
							}

							@Override
							public void onException(Exception exception) {
								if (holder.exceptions == null) {
									holder.exceptions = new Exception[getters.length];
								}
								holder.exceptions[finalI] = exception;
								if (returnOnFirstException && holder.n > 0)
									holder.n = 1;
								checkCompleteResult();
							}
						});
					}

				}
			}
		};
	}

	/**
	 * Returns new  AsyncTask which contains two tasks from argument and executes them parallel
	 */
	public static AsyncTask parallel(AsyncTask task1, AsyncTask task2) {
		return parallel(new AsyncTask[]{task1, task2});
	}

	/**
	 * Returns new  AsyncFunction which contains functions from argument and applies them successively
	 *
	 * @param <I> type of value for input to function
	 * @param <O> type of result
	 */
	public static <I, O> AsyncFunction<I, O> sequence(final AsyncFunction<?, ?>... functions) {
		return new AsyncFunction<I, O>() {
			@SuppressWarnings("unchecked")
			@Override
			public void apply(I input, final ResultCallback<O> callback) {
				if (functions.length == 0) {
					callback.onResult((O) input);
				} else {
					ForwardingResultCallback<Object> internalCallback = new ForwardingResultCallback<Object>(callback) {
						int n = 1;

						@Override
						public void onResult(Object result) {
							if (n == functions.length) {
								callback.onResult((O) result);
							} else {
								AsyncFunction<Object, Object> function = (AsyncFunction<Object, Object>) functions[n++];
								function.apply(result, this);
							}
						}
					};

					AsyncFunction<I, Object> function = (AsyncFunction<I, Object>) functions[0];
					function.apply(input, internalCallback);
				}
			}
		};
	}

	/**
	 * Returns new AsyncFunction which is composition from two functions from argument.
	 * Type of output first function should equals type for input for second function
	 *
	 * @param <F> type of input first function
	 * @param <T> type of output first function and input for second function
	 * @param <O> type of output second function
	 */
	public static <F, T, O> AsyncFunction<F, O> sequence(final AsyncFunction<F, T> function1, final AsyncFunction<T, O> function2) {
		return new AsyncFunction<F, O>() {
			@Override
			public void apply(F input, final ResultCallback<O> callback) {
				function1.apply(input, new ForwardingResultCallback<T>(callback) {
					@Override
					public void onResult(T result) {
						function2.apply(result, new ForwardingResultCallback<O>(callback) {
							@Override
							public void onResult(O result) {
								callback.onResult(result);
							}
						});
					}
				});
			}
		};
	}

	/**
	 * Returns new AsyncTask which set value for AsyncSetter
	 *
	 * @param <T> type of value
	 */
	public static <T> AsyncTask combine(final T value, final AsyncSetter<T> setter) {
		return new AsyncTask() {
			@Override
			public void execute(CompletionCallback callback) {
				setter.set(value, callback);
			}
		};
	}

	/**
	 * Returns new AsyncGetter which with method get() applies from to function
	 *
	 * @param <F> type of value for appplying
	 * @param <T> type of output function
	 */
	public static <F, T> AsyncGetter<T> combine(final F from, final AsyncFunction<F, T> function) {
		return new AsyncGetter<T>() {
			@Override
			public void get(ResultCallback<T> callback) {
				function.apply(from, callback);
			}
		};
	}

	/**
	 * Returns getter which with method get() calls onResult() with value with argument
	 *
	 * @param value value for argument onResult()
	 * @param <T>   type of result
	 */
	public static <T> AsyncGetter<T> constGetter(final T value) {
		return new AsyncGetter<T>() {
			@Override
			public void get(ResultCallback<T> callback) {
				callback.onResult(value);
			}
		};
	}

	public static <T> AsyncGetter<T> combine(AsyncTask asyncTask, T value) {
		return sequence(asyncTask, constGetter(value));
	}

	/**
	 * Returns AsyncTask which during executing calls method get() from getter and after that sets result to setter
	 *
	 * @param <T> type of result
	 */
	public static <T> AsyncTask sequence(final AsyncGetter<T> getter, final AsyncSetter<T> setter) {
		return new AsyncTask() {
			@Override
			public void execute(final CompletionCallback callback) {
				getter.get(new ForwardingResultCallback<T>(callback) {
					@Override
					public void onResult(T result) {
						setter.set(result, callback);
					}
				});
			}
		};
	}

	/**
	 * Returns AsyncTask which during executing calls method get() from getter and after that calls onResult() from resultCallback
	 *
	 * @param <T> type of result
	 */
	public static <T> AsyncTask combine(final AsyncGetter<T> getter, final ResultCallback<T> resultCallback) {
		return new AsyncTask() {
			@Override
			public void execute(final CompletionCallback callback) {
				getter.get(new ForwardingResultCallback<T>(callback) {
					@Override
					public void onResult(T result) {
						resultCallback.onResult(result);
						callback.onComplete();
					}

					@Override
					public void onException(Exception exception) {
						resultCallback.onException(exception);
						callback.onException(exception);
					}
				});
			}
		};
	}

	public static <F, T> AsyncFunction<F, T> sequence(final AsyncSetter<F> setter, final AsyncGetter<T> getter) {
		return new AsyncFunction<F, T>() {
			@Override
			public void apply(F input, final ResultCallback<T> callback) {
				setter.set(input, new ForwardingCompletionCallback(callback) {
					@Override
					public void onComplete() {
						getter.get(callback);
					}
				});
			}
		};
	}

	/**
	 * Returns AsyncGetter which executes AsyncTask from arguments and gets getter
	 *
	 * @param <T> type of result
	 */
	public static <T> AsyncGetter<T> sequence(final AsyncTask task, final AsyncGetter<T> getter) {
		return new AsyncGetter<T>() {
			@Override
			public void get(final ResultCallback<T> callback) {
				task.execute(new ForwardingCompletionCallback(callback) {
					@Override
					public void onComplete() {
						getter.get(callback);
					}
				});
			}
		};
	}

	public static <T> AsyncSetter<T> sequence(final AsyncSetter<T> setter, final AsyncTask task) {
		return new AsyncSetter<T>() {
			@Override
			public void set(T value, final CompletionCallback callback) {
				setter.set(value, new ForwardingCompletionCallback(callback) {
					@Override
					public void onComplete() {
						task.execute(callback);
					}
				});
			}
		};
	}

	public static <F, T> AsyncGetter<T> sequence(final AsyncGetter<F> getter, final AsyncFunction<F, T> function) {
		return new AsyncGetter<T>() {
			@Override
			public void get(final ResultCallback<T> callback) {
				getter.get(new ForwardingResultCallback<F>(callback) {
					@Override
					public void onResult(F result) {
						function.apply(result, callback);
					}
				});
			}
		};
	}

	public static <F, T> AsyncGetter<T> combine(final AsyncGetter<F> getter, final Function<F, T> function) {
		return new AsyncGetter<T>() {
			@Override
			public void get(final ResultCallback<T> callback) {
				getter.get(new ForwardingResultCallback<F>(callback) {
					@Override
					public void onResult(F result) {
						callback.onResult(function.apply(result));
					}
				});
			}
		};
	}

	public static <F, T> AsyncSetter<F> sequence(final AsyncFunction<F, T> function, final AsyncSetter<T> setter) {
		return new AsyncSetter<F>() {
			@Override
			public void set(F value, final CompletionCallback callback) {
				function.apply(value, new ForwardingResultCallback<T>(callback) {
					@Override
					public void onResult(T result) {
						setter.set(result, callback);
					}
				});
			}
		};
	}

	public static <F, T> AsyncSetter<F> combine(final Function<F, T> function, final AsyncSetter<T> setter) {
		return new AsyncSetter<F>() {
			@Override
			public void set(F value, CompletionCallback callback) {
				T to = function.apply(value);
				setter.set(to, callback);
			}
		};
	}

	public static <T> AsyncGetterWithSetter<T> createAsyncGetterWithSetter(Eventloop eventloop) {
		return new AsyncGetterWithSetter<>(eventloop);
	}

	private static final class AsyncCompletionCallback implements CompletionCallback {
		private final int minCompleted;
		private final int totalCount;
		private final CompletionCallback callback;

		private int completed = 0;
		private int exceptions = 0;
		private Exception lastException;

		private AsyncCompletionCallback(int minCompleted, int totalCount, CompletionCallback callback) {
			this.minCompleted = minCompleted;
			this.totalCount = totalCount;
			this.callback = callback;
		}

		@Override
		public void onComplete() {
			++completed;
			completeResult();
		}

		@Override
		public void onException(Exception exception) {
			++exceptions;
			lastException = exception;
			completeResult();
		}

		private void completeResult() {
			if ((exceptions + completed) == totalCount) {
				if (completed >= minCompleted) {
					callback.onComplete();
				} else {
					callback.onException(lastException);
				}
			}
		}
	}

	/**
	 * Calls the callback from argument until number of callings it will be equal to count
	 *
	 * @param count    number of callings before running CompletionCallback
	 * @param callback CompletionCallback for calling
	 * @return new AsyncCompletionCallback which will be save count of callings
	 */
	public static CompletionCallback waitAll(int count,
	                                         CompletionCallback callback) {
		if (count == 0) {
			callback.onComplete();
			return ignoreCompletionCallback();
		}
		return new AsyncCompletionCallback(count, count, callback);
	}

	public static CompletionCallback waitAny(int count, int totalCount,
	                                         CompletionCallback callback) {
		if (count == 0) {
			callback.onComplete();
			return ignoreCompletionCallback();
		}
		return new AsyncCompletionCallback(count, totalCount, callback);
	}

	private static final class CompletionCallbackToFuture implements CompletionCallback {
		private final SettableFuture<Void> future;

		private CompletionCallbackToFuture(SettableFuture<Void> future) {
			this.future = future;
		}

		@Override
		public void onComplete() {
			future.set(null);
		}

		@Override
		public void onException(Exception exception) {
			future.setException(exception);
		}
	}

	private static final class ResultCallbackToFuture<T> implements ResultCallback<T> {
		private final SettableFuture<T> future;

		private ResultCallbackToFuture(SettableFuture<T> future) {
			this.future = future;
		}

		@Override
		public void onResult(T result) {
			future.set(result);
		}

		@Override
		public void onException(Exception exception) {
			future.setException(exception);
		}
	}

	/**
	 * Returns CompletionCallback which created with SettableFuture. After calling onException() it sets it to
	 * this future
	 *
	 * @param future the future for exception monitoring
	 */
	public static CompletionCallback completionCallbackOfFuture(SettableFuture<Void> future) {
		return new CompletionCallbackToFuture(future);
	}

	/**
	 * Returns ResultCallback which created with SettableFuture. After calling onResult() or onException() it sets its
	 * parameters to this future
	 *
	 * @param future the future for result and exception monitoring
	 */
	public static <T> ResultCallback<T> resultCallbackOfFuture(SettableFuture<T> future) {
		return new ResultCallbackToFuture<>(future);
	}

	/**
	 * Sets this NioServer as listen, sets future true if it was successfully, else sets exception which was
	 * threw.
	 *
	 * @param nioServer the NioServer which it sets listen.
	 * @return ListenableFuture with result of setting.
	 */
	public static ListenableFuture<?> listenFuture(final NioServer nioServer) {
		final SettableFuture<Boolean> future = SettableFuture.create();
		nioServer.getNioEventloop().postConcurrently(new Runnable() {
			@Override
			public void run() {
				try {
					nioServer.listen();
					future.set(true);
				} catch (IOException e) {
					future.setException(e);
				}
			}
		});
		return future;
	}

	/**
	 * Closes this NioServer, sets future true if it was successfully.
	 *
	 * @param nioServer the NioServer which it will close.
	 * @return ListenableFuture with result of closing.
	 */
	public static ListenableFuture<?> closeFuture(final NioServer nioServer) {
		final SettableFuture<Boolean> future = SettableFuture.create();
		nioServer.getNioEventloop().postConcurrently(new Runnable() {
			@Override
			public void run() {
				nioServer.close();
				future.set(true);
			}
		});
		return future;
	}

	/**
	 * Starts this NioService, sets future true, if it was successfully, else sets exception which was throwing.
	 *
	 * @param nioService the NioService which will be ran.
	 * @return ListenableFuture with result of starting.
	 */
	public static ListenableFuture<?> startFuture(final NioService nioService) {
		final SettableFuture<Boolean> future = SettableFuture.create();
		nioService.getNioEventloop().postConcurrently(new Runnable() {
			@Override
			public void run() {
				nioService.start(new CompletionCallback() {
					@Override
					public void onComplete() {
						future.set(true);
					}

					@Override
					public void onException(Exception exception) {
						future.setException(exception);
					}
				});
			}
		});
		return future;
	}

	/**
	 * Stops this NioService, sets future true, if it was successfully, else sets exception which was throwing.
	 *
	 * @param nioService the NioService which will be stopped.
	 * @return ListenableFuture with result of stoping.
	 */
	public static ListenableFuture<?> stopFuture(final NioService nioService) {
		final SettableFuture<Boolean> future = SettableFuture.create();
		nioService.getNioEventloop().postConcurrently(new Runnable() {
			@Override
			public void run() {
				nioService.stop(new CompletionCallback() {
					@Override
					public void onComplete() {
						future.set(true);
					}

					@Override
					public void onException(Exception exception) {
						future.setException(exception);
					}
				});
			}
		});
		return future;
	}

	/**
	 * It represents exception which can emerge during parallel execution. It contains results which
	 * have been already received and exception which was threw during execution
	 */
	public static final class ParallelExecutionException extends Exception {
		public final Object[] results;
		public final Exception[] exceptions;

		public ParallelExecutionException(Object[] results, Exception[] exceptions) {
			this.results = results;
			this.exceptions = exceptions;
		}
	}
}

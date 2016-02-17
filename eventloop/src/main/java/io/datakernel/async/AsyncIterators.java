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

import io.datakernel.util.Function;

import java.util.Iterator;

/**
 * This class contains static utility methods that operate on or return objects
 * of type AsyncIterators. Except as noted, each method has a corresponding
 * Iterable-based method.
 */
@SuppressWarnings("ToArrayCallWithZeroLengthArrayArgument")
public final class AsyncIterators {
	private AsyncIterators() {
	}

	/**
	 * Returns the AsyncIterator which in first calling returns value, and after that after calling
	 * next() calls onEnd() from callback
	 *
	 * @param value single value for returning
	 * @param <T>   type of value
	 */
	public static <T> AsyncIterator<T> asyncIteratorOfValue(final T value) {
		return new AsyncIterator<T>() {
			boolean hasNext = true;

			@Override
			public void next(IteratorCallback<T> callback) {
				if (hasNext) {
					hasNext = false;
					callback.onNext(value);
				} else {
					callback.onEnd();
				}
			}
		};
	}

	/**
	 * Returns the AsyncIterable which has only one AsyncIterator which has only one value
	 *
	 * @param value single value for returning
	 * @param <T>   type of value
	 */
	public static <T> AsyncIterable<T> asyncIterableOfValue(final T value) {
		return new AsyncIterable<T>() {
			@Override
			public AsyncIterator<T> asyncIterator() {
				return asyncIteratorOfValue(value);
			}
		};
	}

	/**
	 * Returns the AsyncIterator which iterates result of calling method get() from getter
	 *
	 * @param getter getter for iterate
	 * @param <T>    type of result
	 */
	public static <T> AsyncIterator<T> asyncIteratorOfGetter(final AsyncGetter<T> getter) {
		return new AsyncIterator<T>() {
			boolean hasNext = true;

			@Override
			public void next(final IteratorCallback<T> callback) {
				if (hasNext) {
					hasNext = false;
					getter.get(new ResultCallback<T>() {
						@Override
						public void onResult(T result) {
							callback.onNext(result);
						}

						@Override
						public void onException(Exception exception) {
							callback.onException(exception);
						}
					});
				} else {
					callback.onEnd();
				}
			}
		};
	}

	/**
	 * Returns the AsyncIterable with iterator asyncIteratorOfGetter
	 *
	 * @param getter getter for iterate
	 * @param <T>    type of result
	 */
	public static <T> AsyncIterable<T> asyncIterableOfGetter(final AsyncGetter<T> getter) {
		return new AsyncIterable<T>() {
			@Override
			public AsyncIterator<T> asyncIterator() {
				return asyncIteratorOfGetter(getter);
			}
		};
	}

	/**
	 * Returns the AsyncIterator which can iterate other non-asynchronous  iterator
	 *
	 * @param iterator iterator for iterating
	 * @param <T>      type of elements in iterator
	 */
	public static <T> AsyncIterator<T> asyncIteratorOfIterator(final Iterator<T> iterator) {
		return new AsyncIterator<T>() {
			@Override
			public void next(IteratorCallback<T> callback) {
				if (iterator.hasNext())
					callback.onNext(iterator.next());
				else
					callback.onEnd();
			}
		};
	}

	/**
	 * Returns the AsyncIterable from  other non-asynchronous  iterable
	 *
	 * @param iterable iterable for wrapping
	 * @param <T>      type of elements  in iterator
	 */
	public static <T> AsyncIterable<T> asyncIterableOfIterable(final Iterable<T> iterable) {
		return new AsyncIterable<T>() {
			@Override
			public AsyncIterator<T> asyncIterator() {
				return asyncIteratorOfIterator(iterable.iterator());
			}
		};
	}

	/**
	 * Creates the AsyncIterable with AsyncIterator from argument
	 *
	 * @param asyncIterator AsyncIterator for new AsyncIterable
	 * @param <T>           type of elements  in iterator
	 */
	public static <T> AsyncIterable<T> asyncIterableOfAsyncIterator(final AsyncIterator<T> asyncIterator) {
		return new AsyncIterable<T>() {
			@Override
			public AsyncIterator<T> asyncIterator() {
				return asyncIterator;
			}
		};
	}

	/**
	 * Creates the AsyncIterable with  other non-asynchronous  iterator
	 *
	 * @param iterator other non-asynchronous  iterator for new iterable
	 * @param <T>      type of elements  in iterator
	 */
	public static <T> AsyncIterable<T> asyncIterableOfIterator(final Iterator<T> iterator) {
		return new AsyncIterable<T>() {
			@Override
			public AsyncIterator<T> asyncIterator() {
				return asyncIteratorOfIterator(iterator);
			}
		};
	}

	/**
	 * Creates the AsyncIterator from non-asynchronous  iterator of asyncGetters
	 *
	 * @param asyncGetters non-asynchronous  iterator of asyncGetters
	 * @param <T>          type of result of getter
	 */
	public static <T> AsyncIterator<T> asyncIteratorOfGetters(final Iterator<AsyncGetter<T>> asyncGetters) {
		return new AsyncIterator<T>() {
			@Override
			public void next(final IteratorCallback<T> callback) {
				if (asyncGetters.hasNext()) {
					AsyncGetter<T> asyncGetter = asyncGetters.next();
					asyncGetter.get(new ResultCallback<T>() {
						@Override
						public void onResult(T result) {
							callback.onNext(result);
						}

						@Override
						public void onException(Exception exception) {
							callback.onException(exception);
						}
					});
				} else {
					callback.onEnd();
				}
			}
		};
	}

	/**
	 * Creates the AsyncIterable from non-asynchronous  iterable of asyncGetters
	 *
	 * @param getters non-asynchronous  iterable of asyncGetters
	 * @param <T>     type of result of getter
	 */
	public static <T> AsyncIterable<T> asyncIterableOfGetters(final Iterable<AsyncGetter<T>> getters) {
		return new AsyncIterable<T>() {
			@Override
			public AsyncIterator<T> asyncIterator() {
				return asyncIteratorOfGetters(getters.iterator());
			}
		};
	}

	/**
	 * Creates a new AsyncIterator which is combining of asyncIterators from iterator from argument
	 *
	 * @param asyncIterators asyncIterators for combining
	 * @param <T>            type of elements in iterator
	 */
	public static <T> AsyncIterator<T> concat(final Iterator<AsyncIterator<T>> asyncIterators) {
		return new AsyncIterator<T>() {
			@Override
			public void next(final IteratorCallback<T> callback) {
				if (asyncIterators.hasNext()) {
					final AsyncIterator<T> asyncIterator = asyncIterators.next();
					asyncIterator.next(new IteratorCallback<T>() {
						@Override
						public void onNext(T result) {
							callback.onNext(result);
							asyncIterator.next(this);
						}

						@Override
						public void onEnd() {
							next(callback);
						}

						@Override
						public void onException(Exception e) {
							callback.onException(e);
						}
					});
				} else {
					callback.onEnd();
				}
			}
		};
	}

	/**
	 * Creates a new AsyncIterator which is combining of asyncIterators from iterable from argument
	 *
	 * @param asyncIterators asyncIterators for combining
	 * @param <T>            type of elements in iterator
	 */
	public static <T> AsyncIterator<T> concat(final Iterable<AsyncIterator<T>> asyncIterators) {
		return concat(asyncIterators.iterator());
	}

	/**
	 * Creates a new AsyncIterator which applying function from argument to asyncIterator and calls
	 * callback with result of this function
	 *
	 * @param asyncIterator iterator with initial values
	 * @param function      non-asynchronous function for transforming
	 * @param <F>           type of elements in asyncIterator, and type to input from function
	 * @param <T>           type for output function
	 */
	public static <F, T> AsyncIterator<T> transform(final AsyncIterator<F> asyncIterator, final Function<F, T> function) {
		return new AsyncIterator<T>() {
			@Override
			public void next(final IteratorCallback<T> callback) {
				asyncIterator.next(new IteratorCallback<F>() {
					@Override
					public void onNext(F from) {
						T to = function.apply(from);
						callback.onNext(to);
					}

					@Override
					public void onEnd() {
						callback.onEnd();
					}

					@Override
					public void onException(Exception e) {
						callback.onException(e);
					}
				});
			}
		};
	}

	/**
	 * Creates a new AsyncIterable which is applying function from argument to asyncIterable and calls
	 * callback with result of this function
	 *
	 * @param asyncIterable iterable with initial values
	 * @param function      non-asynchronous function for transforming
	 * @param <F>           type of elements in asyncIterable, and type to input from function
	 * @param <T>           type for output function
	 */
	public static <F, T> AsyncIterable<T> transform(final AsyncIterable<F> asyncIterable, final Function<F, T> function) {
		return new AsyncIterable<T>() {
			@Override
			public AsyncIterator<T> asyncIterator() {
				return transform(asyncIterable.asyncIterator(), function);
			}
		};
	}

	/**
	 * Creates a new AsyncIterator which is applying AsyncFunction from argument to asyncIterator and calls
	 * callback with result of this function
	 *
	 * @param asyncIterator iterator with initial values
	 * @param asyncFunction asynchronous function for transforming
	 * @param <F>           type of elements in asyncIterator, and type to input from function
	 * @param <T>           type for output function
	 */
	public static <F, T> AsyncIterator<T> transform(final AsyncIterator<F> asyncIterator, final AsyncFunction<F, T> asyncFunction) {
		return new AsyncIterator<T>() {
			@Override
			public void next(final IteratorCallback<T> callback) {
				asyncIterator.next(new IteratorCallback<F>() {
					@Override
					public void onNext(F from) {
						asyncFunction.apply(from, new ResultCallback<T>() {
							@Override
							public void onResult(T to) {
								callback.onNext(to);
							}

							@Override
							public void onException(Exception exception) {
								callback.onException(exception);
							}
						});
					}

					@Override
					public void onEnd() {
						callback.onEnd();
					}

					@Override
					public void onException(Exception e) {
						callback.onException(e);
					}
				});
			}
		};
	}

	public static <F, T> AsyncIterator<T> transform(final Iterator<F> iterator, final AsyncFunction<F, T> asyncFunction) {
		return new AsyncIterator<T>() {
			@Override
			public void next(final IteratorCallback<T> callback) {
				if (iterator.hasNext()) {
					F from = iterator.next();
					asyncFunction.apply(from, new ResultCallback<T>() {
						@Override
						public void onResult(T to) {
							callback.onNext(to);
						}

						@Override
						public void onException(Exception exception) {
							callback.onException(exception);
						}
					});
				}
				else
					callback.onEnd();
			}
		};
	}

	/**
	 * Creates a new AsyncIterable which is applying AsyncFunction from argument to asyncIterable and calls
	 * callback with result of this function
	 *
	 * @param asyncIterable iterable with initial values
	 * @param asyncFunction asynchronous function for transforming
	 * @param <F>           type of elements in asyncIterator, and type to input from function
	 * @param <T>           type for output function
	 */
	public static <F, T> AsyncIterable<T> transform(final AsyncIterable<F> asyncIterable, final AsyncFunction<F, T> asyncFunction) {
		return new AsyncIterable<T>() {
			@Override
			public AsyncIterator<T> asyncIterator() {
				return transform(asyncIterable.asyncIterator(), asyncFunction);
			}
		};
	}
}
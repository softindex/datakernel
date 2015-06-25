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

package io.datakernel.stream;

import io.datakernel.async.AsyncGetter;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class StreamConsumers {
	private StreamConsumers() {
	}

	public static <T> Idle<T> idle(Eventloop eventloop) {
		return new Idle<>(eventloop);
	}

	public static <T> Closing<T> closing(Eventloop eventloop) {
		return new Closing<>(eventloop);
	}

	public static <T> ClosingWithError<T> closingWithError(Eventloop eventloop, Exception exception) {
		return new ClosingWithError<>(eventloop, exception);
	}

	/**
	 * Returns {@link ToList} which saves received items in list
	 *
	 * @param eventloop event loop in which will run it
	 * @param list      list with received items
	 * @param <T>       type of item
	 */
	public static <T> ToList<T> toList(Eventloop eventloop, List<T> list) {
		return new ToList<>(eventloop, list);
	}

	/**
	 * Returns {@link ToList} which saves received items in empty list
	 *
	 * @param eventloop event loop in which will run it
	 * @param <T>       type of item
	 */
	public static <T> ToList<T> toList(Eventloop eventloop) {
		return toList(eventloop, new ArrayList<T>());
	}

	/**
	 * Returns {@link ToList} which suspends after each receiving data with saving it to list.
	 *
	 * @param eventloop event loop in which will run it
	 * @param list      list with received items
	 * @param <T>       type of item
	 */
	public static <T> ToList<T> toListOneByOne(Eventloop eventloop, List<T> list) {
		return new ToList<T>(eventloop, list) {
			@Override
			public void onData(T item) {
				super.onData(item);
				upstreamProducer.suspend();
				this.eventloop.post(new Runnable() {
					@Override
					public void run() {
						upstreamProducer.resume();
					}
				});
			}
		};
	}

	/**
	 * Returns {@link ToList} which suspends after each receiving data with saving it to empty list.
	 *
	 * @param eventloop event loop in which will run it
	 * @param <T>       type of item
	 */
	public static <T> ToList<T> toListOneByOne(Eventloop eventloop) {
		return toListOneByOne(eventloop, new ArrayList<T>());
	}

	/**
	 * Returns {@link ToList} which suspends after some random receiving data with saving it to list from
	 * argument.
	 * <p>Useful for unit testing purposes - to make sure upstream producer handles onSuspend and onResume events properly
	 *
	 * @param eventloop event loop in which will run it
	 * @param list      list with received items
	 * @param random    for generate random values of boolean
	 * @param <T>       type of item
	 */
	public static <T> ToList<T> toListRandomlySuspending(Eventloop eventloop, List<T> list, final Random random) {
		return new ToList<T>(eventloop, list) {
			@Override
			public void onData(T item) {
				super.onData(item);
				if (random.nextBoolean()) {
					suspendUpstream();
					this.eventloop.post(new Runnable() {
						@Override
						public void run() {
							resumeUpstream();
						}
					});
				}
			}
		};
	}

	/**
	 * Returns {@link ToList} which suspends after default random receiving data with saving it to list
	 * from argument.
	 *
	 * @param eventloop event loop in which will run it
	 * @param list      list with received items
	 * @param <T>       type of item
	 */
	public static <T> ToList<T> toListRandomlySuspending(Eventloop eventloop, List<T> list) {
		return toListRandomlySuspending(eventloop, list, new Random());
	}

	/**
	 * Returns {@link ToList} which suspends after default random receiving data with saving it to empty list.
	 *
	 * @param eventloop event loop in which will run it
	 * @param <T>       type of item
	 */
	public static <T> ToList<T> toListRandomlySuspending(Eventloop eventloop) {
		return toListRandomlySuspending(eventloop, new ArrayList<T>(), new Random());
	}

	public static <T> StreamConsumer<T> asynchronouslyResolving(final Eventloop eventloop, final AsyncGetter<StreamConsumer<T>> consumerGetter) {
		final StreamForwarder<T> forwarder = new StreamForwarder<>(eventloop);
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				consumerGetter.get(new ResultCallback<StreamConsumer<T>>() {
					@Override
					public void onResult(StreamConsumer<T> result) {
						forwarder.streamTo(result);
					}

					@Override
					public void onException(Exception exception) {
						forwarder.streamTo(new ClosingWithError<T>(eventloop, exception));
					}
				});
			}
		});
		return forwarder;
	}

	/**
	 * Represents a simple {@link AbstractStreamConsumer} which with changing producer sets its status as complete.
	 *
	 * @param <T> type of received data
	 */
	public static class Closing<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
		public Closing(Eventloop eventloop) {
			super(eventloop);
		}

		/**
		 * With changing producer sets its status as complete.
		 */
		@Override
		public void onConsumerStarted() {
			upstreamProducer.close();
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return this;
		}

		@Override
		public void onData(T item) {
			// do nothing
		}

		@Override
		public void onEndOfStream() {
		}
	}

	public static class ClosingWithError<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
		private final Exception exception;

		public ClosingWithError(Eventloop eventloop, Exception exception) {
			super(eventloop);
			this.exception = exception;
		}

		@Override
		public void onConsumerStarted() {
			upstreamProducer.closeWithError(exception);
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return this;
		}

		@Override
		public void onData(T item) {
			// do nothing
		}

		@Override
		public void onEndOfStream() {
		}
	}

	/**
	 * Represents a simple {@link AbstractStreamConsumer} which with changing producer sets its status as complete.
	 *
	 * @param <T> type of received data
	 */
	public static class Idle<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {

		public Idle(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return this;
		}

		@Override
		public void onData(T item) {
			// do nothing
		}

		@Override
		public void onEndOfStream() {
			upstreamProducer.close();
		}
	}

	/**
	 * Represents a {@link AbstractStreamConsumer} which adds each item of received data to List.
	 *
	 * @param <T> type of received data
	 */
	public static class ToList<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
		private final List<T> list;

		/**
		 * Creates a new instance of ConsumerToList with empty list and event loop from argument, in which
		 * this customer will run
		 */
		public ToList(Eventloop eventloop) {
			this(eventloop, new ArrayList<T>());
		}

		/**
		 * Creates a new instance of ConsumerToList with
		 *
		 * @param eventloop event loop in which this customer will run
		 * @param list      list for adding received items
		 */
		public ToList(Eventloop eventloop, List<T> list) {
			super(eventloop);
			checkNotNull(list);
			this.list = list;
		}

		/**
		 * Returns list with received items
		 */
		public final List<T> getList() {
			checkState(upstreamProducer.getError() == null, "Upstream error %s: %s", upstreamProducer, upstreamProducer.getError());
			checkState(upstreamProducer.getStatus() == StreamProducer.CLOSED, "Upstream %s is not closed", upstreamProducer);
			return list;
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return this;
		}

		/**
		 * Processes received item and adds it to list
		 *
		 * @param item received item
		 */
		@Override
		public void onData(T item) {
			list.add(item);
		}

		/**
		 * Sets the flag complete as true
		 */
		@Override
		public void onEndOfStream() {
			upstreamProducer.close();
		}
	}
}

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class StreamConsumers {
	private StreamConsumers() {
	}

	public static <T> Idle<T> idle(Eventloop eventloop) {
		return new Idle<>(eventloop);
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
		return new ToList<T>(eventloop, list);
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

	public static final class ClosingWithError<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
		protected static final Logger logger = LoggerFactory.getLogger(ClosingWithError.class);
		private final Exception exception;

		public ClosingWithError(Eventloop eventloop, Exception exception) {
			super(eventloop);
			this.exception = exception;
		}

		@Override
		protected void onStarted() {
			logger.info("Closing with error {}", exception.toString());
			closeWithError(exception);
		}

		@Override
		protected void onEndOfStream() {
		}

		@Override
		protected void onError(Exception e) {
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return new StreamDataReceiver<T>() {
				@Override
				public void onData(T item) {
					throw new RuntimeException("Extra item to ClosingWithError consumer");
				}
			};
		}

		@Override
		public void onData(T item) {

		}
	}

	/**
	 * Represents a simple {@link AbstractStreamConsumer} which with changing producer sets its status as complete.
	 *
	 * @param <T> type of received data
	 */
	public static final class Idle<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {

		protected Idle(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onStarted() {

		}

		@Override
		protected void onEndOfStream() {

		}

		@Override
		protected void onError(Exception e) {

		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return this;
		}

		@Override
		public void onData(T item) {

		}
	}

	/**
	 * Represents a {@link AbstractStreamConsumer} which adds each item of received data to List.
	 *
	 * @param <T> type of received data
	 */
	public static final class ToList<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
		protected final List<T> list;

		/**
		 * Creates a new instance of ConsumerToList with empty list and event loop from argument, in which
		 * this customer will run
		 */
		public ToList(Eventloop eventloop) {
			this(eventloop, new ArrayList<T>());
		}

		@Override
		protected void onStarted() {

		}

		@Override
		protected void onEndOfStream() {
			close();
		}

		@Override
		protected void onError(Exception e) {
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
			checkState(getStatus() == StreamConsumerStatus.CLOSED, "ToList consumer is not closed");
			return list;
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return this;
		}

		@Override
		public void onData(T item) {
			list.add(item);
		}
	}

	public static final class ToListSuspend<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
		private List<T> list;
		private boolean endOfStream;

		protected ToListSuspend(Eventloop eventloop) {
			this(eventloop, new ArrayList<T>());
		}

		public ToListSuspend(Eventloop eventloop, List<T> list) {
			super(eventloop);
			this.list = list;
		}

		@Override
		protected void onStarted() {

		}

		@Override
		protected void onEndOfStream() {
			endOfStream = true;
			close();
		}

		@Override
		protected void onError(Exception e) {
			closeWithError(e);
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return this;
		}

		@Override
		public void onData(T item) {
			list.add(item);
			suspend();
		}

		public final List<T> getList() {
			return list;
		}

		public boolean isEndOfStream() {
			return endOfStream;
		}

		public Exception getOnError() {
			return error;
		}
	}

	public static <T> ToListSuspend<T> toListSuspend(Eventloop eventloop, List<T> list) {
		return new ToListSuspend<T>(eventloop, list);
	}

	public static <T> ToListSuspend<T> toListSuspend(Eventloop eventloop) {
		return toListSuspend(eventloop, new ArrayList<T>());
	}

/*
	public static class TransformerWithoutEnd<I, O> extends AbstractStreamTransformer_1_1_Stateless<I, O> implements StreamDataReceiver<I> {
		public TransformerWithoutEnd(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected StreamDataReceiver<I> getUpstreamDataReceiver() {
			return this;
		}

		@Override
		protected void onUpstreamEndOfStream() {
			downstreamProducer.sendEndOfStream();
		}

		public void closeOnComplete() {
			upstreamConsumer.close();
		}

		public void closeOnError(Exception e) {
			upstreamConsumer.closeWithError(e);
		}

		@Override
		public void onData(I item) {
			downstreamDataReceiver.onData((O) item);
		}
	}

	public static <I, O> TransformerWithoutEnd<I, O> transformerWithoutEnd(Eventloop eventloop) {
		return new TransformerWithoutEnd<>(eventloop);
	}
*/
}

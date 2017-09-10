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

import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.Eventloop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.datakernel.async.AsyncCallbacks.throwableToException;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public final class StreamConsumers {
	private StreamConsumers() {
	}

	public static <T> Idle<T> idle() {
		return new Idle<>();
	}

	public static <T> ClosingWithError<T> closingWithError(Eventloop eventloop, Exception exception) {
		return new ClosingWithError<>(eventloop, exception);
	}

	public static <T> StreamConsumer<T> ofStage(CompletionStage<StreamConsumer<T>> consumerStage) {
		StreamConsumerDecorator<T> decorator = StreamConsumerDecorator.create();
		consumerStage
				.exceptionally(throwable ->
						closingWithError(getCurrentEventloop(), throwableToException(throwable)))
				.thenAccept(decorator::setActualConsumer);
		return decorator;
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

	public static final class ClosingWithError<T> extends AbstractStreamConsumer<T> {
		protected static final Logger logger = LoggerFactory.getLogger(ClosingWithError.class);
		private final Exception exception;

		public ClosingWithError(Eventloop eventloop, Exception exception) {
			super(eventloop);
			this.exception = exception;
		}

		@Override
		protected void onError(Exception e) {
		}

		@Override
		protected void onStarted() {
			logger.trace("Closing with error {}", exception.toString());
			closeWithError(exception);
		}

		@Override
		protected void onEndOfStream() {
		}
	}

	/**
	 * Represents a simple {@link AbstractStreamConsumer} which with changing producer sets its status as complete.
	 *
	 * @param <T> type of received data
	 */
	public static final class Idle<T> implements StreamConsumer<T> {
		private StreamProducer<T> producer;

		@Override
		public void streamFrom(StreamProducer<T> producer) {
			checkNotNull(producer);
			if (this.producer == producer) return;

			checkState(this.producer == null);

			this.producer = producer;
			producer.streamTo(this);
		}

		@Override
		public void endOfStream() {
		}

		@Override
		public void closeWithError(Exception e) {
			producer.closeWithError(e);
		}

		public StreamProducer<T> getProducer() {
			return producer;
		}
	}

	/**
	 * Represents a {@link AbstractStreamConsumer} which adds each item of received data to List.
	 *
	 * @param <T> type of received data
	 */
	public static final class ToList<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
		protected final List<T> list;
		private SettableStage<List<T>> resultStage;

		/**
		 * Creates a new instance of ConsumerToList with empty list and event loop from argument, in which
		 * this customer will run
		 */
		public ToList(Eventloop eventloop) {
			this(eventloop, new ArrayList<>());
		}

		public CompletionStage<List<T>> getResultStage() {
			if (getStatus().isOpen()) {
				return this.resultStage = SettableStage.create();
			} else {
				if (getException() != null) {
					return SettableStage.immediateFailedStage(getException());
				} else {
					return SettableStage.immediateStage(null);
				}
			}
		}

		@Override
		protected void onStarted() {
			getProducer().produce(this);
		}

		@Override
		protected void onEndOfStream() {
			if (resultStage != null) {
				resultStage.set(list);
			}
		}

		@Override
		protected void onError(Exception e) {
			if (resultStage != null) {
				resultStage.setException(e);
			}
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
			checkState(getStatus() == StreamStatus.END_OF_STREAM, "ToList consumer is not closed");
			return list;
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
			this(eventloop, new ArrayList<>());
		}

		public ToListSuspend(Eventloop eventloop, List<T> list) {
			super(eventloop);
			this.list = list;
		}

		@Override
		protected void onStarted() {
			getProducer().produce(this);
		}

		@Override
		protected void onEndOfStream() {
			endOfStream = true;
		}

		@Override
		protected void onError(Exception e) {
			closeWithError(e);
		}

		@Override
		public void onData(T item) {
			list.add(item);
			getProducer().suspend();
		}

		public final List<T> getList() {
			return list;
		}

		public boolean isEndOfStream() {
			return endOfStream;
		}

		public Exception getOnError() {
			return getException();
		}
	}

	public static <T> ToListSuspend<T> toListSuspend(Eventloop eventloop, List<T> list) {
		return new ToListSuspend<>(eventloop, list);
	}

	public static <T> ToListSuspend<T> toListSuspend(Eventloop eventloop) {
		return toListSuspend(eventloop, new ArrayList<T>());
	}

}

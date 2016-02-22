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

import io.datakernel.annotation.Nullable;
import io.datakernel.eventloop.Eventloop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.stream.StreamStatus.*;

/**
 * It is basic implementation of {@link StreamProducer}
 *
 * @param <T> type of received item
 */
public abstract class AbstractStreamProducer<T> implements StreamProducer<T> {
	private static final Logger logger = LoggerFactory.getLogger(AbstractStreamProducer.class);

	protected final Eventloop eventloop;

	protected final List<T> bufferedList = new ArrayList<>();
	protected StreamConsumer<T> downstreamConsumer;
	protected StreamDataReceiver<T> downstreamDataReceiver = new DataReceiverBeforeStart<>(this, bufferedList);

	private StreamStatus status = READY;
	private boolean ready = true;
	protected Exception error;

	protected Object tag;

	protected AbstractStreamProducer(Eventloop eventloop) {
		this.eventloop = checkNotNull(eventloop);
	}

	private boolean rewiring;

	/**
	 * Sets consumer for this producer. At the moment of calling this method producer shouldn't have consumer,
	 * as well as consumer shouldn't have producer, otherwise there will be error
	 *
	 * @param downstreamConsumer consumer for streaming
	 */
	@Override
	public final void streamTo(final StreamConsumer<T> downstreamConsumer) {
		checkNotNull(downstreamConsumer);
		if (rewiring || this.downstreamConsumer == downstreamConsumer)
			return;
		rewiring = true;

		boolean firstTime = this.downstreamConsumer == null;

		if (this.downstreamConsumer != null) {
			this.downstreamConsumer.streamFrom(StreamProducers.<T>closingWithError(eventloop,
					new Exception("Downstream disconnected")));
		}

		this.downstreamConsumer = downstreamConsumer;

		downstreamConsumer.streamFrom(this);

		bindDataReceiver();

		if (firstTime && bufferedList.size() != 0) {
			logger.trace("{} Send buffered items", this);
			for (T item : bufferedList) {
				downstreamConsumer.getDataReceiver().onData(item);
			}
			bufferedList.clear();
		}

		if (status == END_OF_STREAM) {
			downstreamConsumer.onProducerEndOfStream();
			return;
		}

		if (status == CLOSED_WITH_ERROR) {
			downstreamConsumer.onProducerError(error);
			return;
		}

		if (firstTime) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					if (status.isOpen()) {
						onStarted();
					}
				}
			});
		}
		rewiring = false;
	}

	/**
	 * Sends {@code item} to consumer
	 *
	 * @param item item to be sent
	 */
	protected void send(T item) {
		assert status.isOpen();
		downstreamDataReceiver.onData(item);
	}

	protected void doProduce() {
	}

	protected void produce() {
		if (!isStatusReady())
			return;
		try {
			doProduce();
		} catch (Exception e) {
			closeWithError(e, true);
		}
	}

	protected void resumeProduce() {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				produce();
			}
		});
	}

	protected void onStarted() {

	}

	@Nullable
	public final StreamConsumer<T> getDownstream() {
		return downstreamConsumer;
	}

	/**
	 * Connects consumer's {@link StreamDataReceiver} to producer
	 */
	@Override
	public final void bindDataReceiver() {
		if (status.isClosed()) {
			downstreamDataReceiver = new DataReceiverAfterClose<>();
			return;
		}

		StreamDataReceiver<T> newDataReceiver = downstreamConsumer.getDataReceiver();
		assert newDataReceiver != null;
		if (downstreamDataReceiver != newDataReceiver) {
			downstreamDataReceiver = newDataReceiver;
			onDataReceiverChanged();
		}
	}

	protected abstract void onDataReceiverChanged();

	public StreamDataReceiver<T> getDownstreamDataReceiver() {
		return downstreamDataReceiver;
	}

	abstract protected void onSuspended();

	@Override
	public final void onConsumerSuspended() {
		if (status != READY)
			return;
		setStatus(SUSPENDED);
		onSuspended();
	}

	abstract protected void onResumed();

	@Override
	public final void onConsumerResumed() {
		if (status != SUSPENDED)
			return;
		setStatus(READY);
		onResumed();
	}

	protected void sendEndOfStream() {
		if (status.isClosed())
			return;
		setStatus(END_OF_STREAM);
		downstreamDataReceiver = new DataReceiverAfterClose<>();
		if (downstreamConsumer != null) {
			downstreamConsumer.onProducerEndOfStream();
		}
		doCleanup();
	}

	private void closeWithError(Exception e, boolean sendToConsumer) {
		if (status.isClosed())
			return;
		setStatus(CLOSED_WITH_ERROR);
		error = e;
		downstreamDataReceiver = new DataReceiverAfterClose<>();
		logger.error("StreamProducer {} closed with error {}", this, error.toString());
		if (sendToConsumer && downstreamConsumer != null) {
			downstreamConsumer.onProducerError(e);
		}
		onError(e);
		doCleanup();
	}

	protected void closeWithError(Exception e) {
		closeWithError(e, true);
	}

	@Override
	public final void onConsumerError(Exception e) {
		closeWithError(e, false);
	}

	protected void onError(Exception e) {

	}

	protected void doCleanup() {
	}

	@Override
	public final StreamStatus getProducerStatus() {
		return status;
	}

	@Override
	public final Exception getProducerException() {
		return error;
	}

	private void setStatus(StreamStatus status) {
		this.status = status;
		this.ready = status == READY;
	}

	public final boolean isStatusReady() {
		return ready;
	}

	public final Object getTag() {
		return tag;
	}

	public final void setTag(Object tag) {
		this.tag = tag;
	}

	@Override
	public String toString() {
		return tag != null ? tag.toString() : super.toString();
	}

	private static class DataReceiverBeforeStart<T> implements StreamDataReceiver<T> {
		private final AbstractStreamProducer self;
		private final List<T> list;

		private DataReceiverBeforeStart(AbstractStreamProducer self, List<T> list) {
			this.self = self;
			this.list = list;
		}

		@Override
		public void onData(T item) {
			logger.trace("{} add item to buffer", self);
			self.onConsumerSuspended();
			list.add(item);
		}
	}

	private static class DataReceiverAfterClose<T> implements StreamDataReceiver<T> {
		@Override
		public void onData(T item) {
			logger.error("Unexpected item {} after end-of-stream of {}", item, this);
		}
	}
}

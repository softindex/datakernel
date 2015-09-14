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

package io.datakernel.stream.processor;

import com.google.common.base.Function;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamProducer;
import io.datakernel.stream.AbstractStreamTransformer_1_N;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.arraycopy;

/**
 * It is {@link AbstractStreamTransformer_1_N} which divides input stream  into groups with some key
 * function, and sends obtained streams to consumers.
 *
 * @param <K> type of result key function
 * @param <T> type of input items
 */
@SuppressWarnings("unchecked")
public final class StreamSharder<K, T> extends AbstractStreamTransformer_1_N<T> implements StreamDataReceiver<T>, StreamSharderMBean {
	private final Sharder<K> sharder;
	private final Function<T, K> keyFunction;
	private StreamDataReceiver<T>[] dataReceivers = new StreamDataReceiver[]{};

	private long jmxItems;

	public class InternalProducer extends AbstractStreamProducer<T> {
		public InternalProducer(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		public void bindDataReceiver() {
			super.bindDataReceiver();
			dataReceivers[outputs.indexOf(this)] = downstreamDataReceiver;
		}

		@Override
		protected void onSuspended() {
			suspendUpstream();
		}

		@Override
		protected void onResumed() {
			if (allOutputsResumed()) {
				resumeUpstream();
			}
		}

		@Override
		protected void onClosed() {
			closeUpstream();
		}

		@Override
		protected void onClosedWithError(Exception e) {
			onProducerError(e);
			downstreamConsumer.onProducerError(e);
		}
	}

	public StreamSharder(Eventloop eventloop, Sharder<K> sharder, Function<T, K> keyFunction) {
		super(eventloop);
		this.sharder = checkNotNull(sharder);
		this.keyFunction = checkNotNull(keyFunction);
	}

	public StreamProducer<T> newOutput() {
		InternalProducer newOutput = new InternalProducer(eventloop);
		addOutput(newOutput);

		StreamDataReceiver<T>[] newDataReceivers = new StreamDataReceiver[dataReceivers.length + 1];
		arraycopy(dataReceivers, 0, newDataReceivers, 0, dataReceivers.length);
		dataReceivers = newDataReceivers;

		return newOutput;
	}

	@Override
	public void onProducerEndOfStream() {
		sendEndOfStreamToDownstreams();
	}

	@Override
	public StreamDataReceiver<T> getDataReceiver() {
		return this;
	}

	/**
	 * After receiving item, finds result for key function for item and streams to corresponding
	 * it consumer
	 *
	 * @param item receiving item
	 */
	@Override
	public void onData(T item) {
		assert jmxItems != ++jmxItems;
		K key = keyFunction.apply(item);
		int shard = sharder.shard(key);
		StreamDataReceiver<T> streamCallback = dataReceivers[shard];
		streamCallback.onData(item);
	}

	@Override
	public long getItems() {
		return jmxItems;
	}

	@SuppressWarnings("AssertWithSideEffects")
	@Override
	public String toString() {
		String items = "?";
		assert (items = "" + jmxItems) != null;
		return '{' + super.toString() + " items:" + items + '}';
	}
}

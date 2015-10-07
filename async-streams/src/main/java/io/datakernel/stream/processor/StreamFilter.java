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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.AbstractStreamTransformer_1_1_Stateless;
import io.datakernel.stream.StreamDataReceiver;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides you to filter data for sending. It checks predicate's verity for inputting data and if
 * predicate is true sends data to the destination. It is a {@link AbstractStreamTransformer_1_1}
 * which receives specified type and streams result set to the destination .
 *
 * @param <T>
 */
public final class StreamFilter<T> extends AbstractStreamTransformer_1_1_Stateless<T, T> implements StreamDataReceiver<T>, StreamFilterMBean {
	private final Predicate<T> predicate;

	private int jmxInputItems;
	private int jmxOutputItems;

	/**
	 * Creates a new instance of this class
	 *
	 * @param eventloop eventloop in which filter will be running
	 * @param predicate predicate for filtering data
	 */
	public StreamFilter(Eventloop eventloop, Predicate<T> predicate) {
		super(eventloop);
		checkNotNull(predicate);
		this.predicate = predicate;
	}

	/**
	 * Returns callback for right sending data, if its predicate always is true, returns dataReceiver
	 * for sending data without filtering.
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected StreamDataReceiver<T> getUpstreamDataReceiver() {
		return predicate == Predicates.alwaysTrue() ? downstreamDataReceiver : this;
	}

	@Override
	protected void onUpstreamEndOfStream() {
		downstreamProducer.sendEndOfStream();
		upstreamConsumer.close();
	}

	/**
	 * Checks  predicate's verity and if it is true, sends data to the destination.
	 *
	 * @param item received data
	 */
	@Override
	public void onData(T item) {
		assert jmxInputItems != ++jmxInputItems;
		if (predicate.apply(item)) {
			assert jmxOutputItems != ++jmxOutputItems;
			downstreamDataReceiver.onData(item);
		}
	}

	@Override
	public int getInputItems() {
		return jmxInputItems;
	}

	@Override
	public int getOutputItems() {
		return jmxOutputItems;
	}

	@SuppressWarnings("AssertWithSideEffects")
	@Override
	public String toString() {
		String in = "?";
		String out = "?";
		assert (in = "" + jmxInputItems) != null;
		assert (out = "" + jmxOutputItems) != null;
		return '{' + super.toString() + " in:" + in + " out:" + out + '}';
	}

	//for test only
	byte getUpstreamConsumerStatus() {
		return upstreamConsumer.getStatus();
	}

	byte getDownstreamProducerStatus() {
		return downstreamProducer.getStatus();
	}
}

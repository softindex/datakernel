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
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides you to filter data for sending. It checks predicate's verity for inputting data and if
 * predicate is true sends data to the destination. It is a {@link AbstractStreamTransformer_1_1}
 * which receives specified type and streams result set to the destination .
 *
 * @param <T>
 */
public final class StreamFilter<T> extends AbstractStreamTransformer_1_1<T, T> implements EventloopJmxMBean {
	private final InputConsumer inputConsumer;
	private final OutputProducer outputProducer;

	// region creators
	private StreamFilter(Eventloop eventloop, Predicate<T> predicate) {
		super(eventloop);
		checkNotNull(predicate);
		this.inputConsumer = new InputConsumer();
		this.outputProducer = new OutputProducer(predicate);
	}

	/**
	 * Creates a new instance of this class
	 *
	 * @param eventloop eventloop in which filter will be running
	 * @param predicate predicate for filtering data
	 */
	public static <T> StreamFilter<T> create(Eventloop eventloop, Predicate<T> predicate) {
		return new StreamFilter<>(eventloop, predicate);
	}
	// endregion

	protected final class InputConsumer extends AbstractInputConsumer {

		@Override
		protected void onUpstreamEndOfStream() {
			outputProducer.sendEndOfStream();
		}

		@SuppressWarnings("unchecked")
		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return outputProducer.predicate == Predicates.alwaysTrue() ?
					outputProducer.getDownstreamDataReceiver() :
					outputProducer;
		}
	}

	protected final class OutputProducer extends AbstractOutputProducer implements StreamDataReceiver<T> {
		private final Predicate<T> predicate;

		private int jmxInputItems;
		private int jmxOutputItems;

		public OutputProducer(Predicate<T> predicate) {this.predicate = predicate;}

		@Override
		protected void onDownstreamSuspended() {
			inputConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			inputConsumer.resume();
		}

		@SuppressWarnings("AssertWithSideEffects")
		@Override
		public void onData(T item) {
			assert jmxInputItems != ++jmxInputItems;
			if (predicate.apply(item)) {
				assert jmxOutputItems != ++jmxOutputItems;
				send(item);
			}
		}
	}

	// jmx
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
	public int getInputItems() {
		return outputProducer.jmxInputItems;
	}

	@JmxAttribute
	public int getOutputItems() {
		return outputProducer.jmxOutputItems;
	}

	@SuppressWarnings("AssertWithSideEffects")
	@Override
	public String toString() {
		String in = "?";
		String out = "?";
		assert (in = "" + outputProducer.jmxInputItems) != null;
		assert (out = "" + outputProducer.jmxOutputItems) != null;
		return '{' + super.toString() + " in:" + in + " out:" + out + '}';
	}
}

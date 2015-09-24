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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_N;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;

import static java.lang.System.arraycopy;

@SuppressWarnings("unchecked")
public class StreamSplitter<T> extends AbstractStreamTransformer_1_N<T> implements StreamSplitterMBean {
	private int jmxItems;

	public StreamSplitter(Eventloop eventloop) {
		super(eventloop);
		this.upstreamConsumer = new UpstreamConsumer();
	}

	private final class UpstreamConsumer extends AbstractUpstreamConsumer implements StreamDataReceiver<T>{

		@Override
		protected void onUpstreamStarted() {

		}

		@Override
		protected void onUpstreamEndOfStream() {
			for (AbstractDownstreamProducer<?> downstreamProducer : downstreamProducers) {
				downstreamProducer.sendEndOfStream();
			}
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return this;
		}

		@Override
		public void onData(T item) {
			assert jmxItems != ++jmxItems;
			for (StreamDataReceiver<T> streamCallback : (StreamDataReceiver<T>[]) dataReceivers) {
				streamCallback.onData(item);
			}
		}
	}

	protected final class DownstreamProducer extends AbstractDownstreamProducer<T> {

		@Override
		protected final void onDownstreamSuspended() {
			upstreamConsumer.getUpstream().onConsumerSuspended();
		}

		@Override
		protected final void onDownstreamResumed() {
			if (allOutputsResumed()) {
				upstreamConsumer.getUpstream().onConsumerResumed();
			}
		}

		@Override
		protected final void onStarted() {

		}
	}

	public StreamProducer<T> newOutput() {
		return addOutput(new DownstreamProducer());
	}

	@Override
	public int getItems() {
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

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
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;

@SuppressWarnings("unchecked")
public final class StreamSplitter<T> extends AbstractStreamSplitter<T> implements EventloopJmxMBean {
	private int jmxItems;

	// region creators
	private StreamSplitter(Eventloop eventloop) {
		super(eventloop);
		this.inputConsumer = new InputConsumer() {
			@Override
			public void onData(T item) {
				assert jmxItems != ++jmxItems;
				for (StreamDataReceiver<T> streamCallback : (StreamDataReceiver<T>[]) dataReceivers) {
					streamCallback.onData(item);
				}
			}
		};
	}

	public static <T> StreamSplitter<T> create(Eventloop eventloop) {return new StreamSplitter<T>(eventloop);}
	// endregion

	public StreamProducer<T> newOutput() {
		return addOutput(new OutputProducer<T>());
	}

	// jmx

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
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

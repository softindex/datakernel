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
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.AbstractStreamTransformer_N_1;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamDataReceiver;

/**
 * It is {@link AbstractStreamTransformer_1_1} which unions all input streams and streams it
 * combination to the destination.
 *
 * @param <T> type of output data
 */
public final class StreamUnion<T> extends AbstractStreamTransformer_N_1<T> {
	public StreamUnion(Eventloop eventloop) {
		super(eventloop);
		this.downstreamProducer = new DownstreamProducer();
	}

	protected final class UpstreamConsumer extends AbstractUpstreamConsumer<T> implements StreamDataReceiver<T> {
		private final DownstreamProducer downstreamProducer = (DownstreamProducer) StreamUnion.this.downstreamProducer;

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return downstreamProducer.getDownstreamDataReceiver() != null
					? downstreamProducer.getDownstreamDataReceiver()
					: this;
		}

		@Override
		public void onData(T item) {
			downstreamProducer.send(item);
		}

		@Override
		protected void onUpstreamStarted() {
		}

		@Override
		protected void onUpstreamEndOfStream() {
			if (allUpstreamsEndOfStream()) {
				downstreamProducer.sendEndOfStream();
			}
		}
	}

	public final class DownstreamProducer extends AbstractDownstreamProducer {
		@Override
		protected void onDownstreamStarted() {
		}

		@Override
		protected void onDownstreamSuspended() {
			suspendAllUpstreams();
		}

		@Override
		protected void onDownstreamResumed() {
			resumeAllUpstreams();
		}

	}

	public StreamConsumer<T> newInput() {
		return addInput(new UpstreamConsumer());
	}
}

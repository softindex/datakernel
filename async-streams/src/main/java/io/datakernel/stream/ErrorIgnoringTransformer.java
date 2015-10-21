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

import io.datakernel.eventloop.Eventloop;

public class ErrorIgnoringTransformer<T> extends AbstractStreamTransformer_1_1<T, T> {
	private UpstreamConsumer upstreamConsumer;
	private DownstreamProducer downstreamProducer;

	private class UpstreamConsumer extends AbstractUpstreamConsumer {

		@Override
		protected void onUpstreamEndOfStream() {
			downstreamProducer.sendEndOfStream();
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return downstreamProducer.getDownstreamDataReceiver();
		}

		@Override
		protected void onError(Exception e) {
			downstreamProducer.sendEndOfStream();
		}
	}

	private class DownstreamProducer extends AbstractDownstreamProducer {

		@Override
		protected void onDownstreamSuspended() {
			upstreamConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			upstreamConsumer.resume();
		}
	}

	public ErrorIgnoringTransformer(Eventloop eventloop) {
		super(eventloop);
		upstreamConsumer = new UpstreamConsumer();
		downstreamProducer = new DownstreamProducer();
	}

}
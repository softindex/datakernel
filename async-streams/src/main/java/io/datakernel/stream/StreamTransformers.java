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

public class StreamTransformers {
	public static class TransformerWithoutEnd<I> extends AbstractStreamTransformer_1_1<I, I> {
		private UpstreamConsumer upstreamConsumer;
		private DownstreamProducer downstreamProducer;

		public TransformerWithoutEnd(Eventloop eventloop) {
			super(eventloop);
			upstreamConsumer = new UpstreamConsumer();
			downstreamProducer = new DownstreamProducer();
		}

		private class UpstreamConsumer extends AbstractUpstreamConsumer {

			@Override
			protected void onUpstreamStarted() {

			}

			@Override
			protected void onUpstreamEndOfStream() {
				downstreamProducer.sendEndOfStream();
			}

			@Override
			public StreamDataReceiver<I> getDataReceiver() {
				return downstreamProducer.getDownstreamDataReceiver();
			}
		}

		private class DownstreamProducer extends AbstractDownstreamProducer {

			@Override
			protected void onDownstreamStarted() {

			}

			@Override
			protected void onDownstreamSuspended() {
				upstreamConsumer.suspend();
			}

			@Override
			protected void onDownstreamResumed() {
				upstreamConsumer.resume();
			}

			@Override
			protected void onEndOfStream() {

			}
		}

		public void closeOnComplete() {
			upstreamConsumer.close();
		}

		public void closeOnError(Exception e) {
			upstreamConsumer.closeWithError(e);
		}

	}

	public static <I> TransformerWithoutEnd<I> transformerWithoutEnd(Eventloop eventloop) {
		return new TransformerWithoutEnd<>(eventloop);
	}

}

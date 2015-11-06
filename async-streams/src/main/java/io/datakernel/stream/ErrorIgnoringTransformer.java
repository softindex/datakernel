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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorIgnoringTransformer<T> extends AbstractStreamTransformer_1_1<T, T> {
	private static final Logger logger = LoggerFactory.getLogger(ErrorIgnoringTransformer.class);

	private InputConsumer inputConsumer;
	private OutputProducer outputProducer;

	private class InputConsumer extends AbstractInputConsumer {

		@Override
		protected void onUpstreamEndOfStream() {
			outputProducer.sendEndOfStream();
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return outputProducer.getDownstreamDataReceiver();
		}

		@Override
		protected void onError(Exception e) {
			logger.info("Ignoring exception", e);
			outputProducer.sendEndOfStream();
		}
	}

	private class OutputProducer extends AbstractOutputProducer {

		@Override
		protected void onDownstreamSuspended() {
			inputConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			inputConsumer.resume();
		}
	}

	public ErrorIgnoringTransformer(Eventloop eventloop) {
		super(eventloop);
		inputConsumer = new InputConsumer();
		outputProducer = new OutputProducer();
	}

}
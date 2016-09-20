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

public final class ErrorIgnoringTransformer<T> extends AbstractStreamTransformer_1_1<T, T> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private InputConsumer inputConsumer;
	private OutputProducer outputProducer;

	// region creators
	private ErrorIgnoringTransformer(Eventloop eventloop) {
		super(eventloop);
		inputConsumer = new InputConsumer();
		outputProducer = new OutputProducer();
	}

	public static <T> ErrorIgnoringTransformer<T> create(Eventloop eventloop) {
		return new ErrorIgnoringTransformer<>(eventloop);
	}
	// endregion

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
			logger.warn("Ignoring exception", e);
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
}
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

/**
 * See also:
 * <br><a href="http://en.wikipedia.org/wiki/Decorator_pattern">Decorator Pattern</a>
 * <br>{@link com.google.common.collect.ForwardingObject}
 * <br>{@link com.google.common.collect.ForwardingCollection}
 *
 * @param <T> item type
 */
public class StreamConsumerDecorator<T> implements StreamConsumer<T> {

	private HasInput<T> input;

	public StreamConsumerDecorator() {
	}

	public StreamConsumerDecorator(HasInput<T> input) {
		this.input = input;
	}

	public void setInput(HasInput<T> input) {
		this.input = input;
	}

	@Override
	public StreamDataReceiver<T> getDataReceiver() {
		return input.getInput().getDataReceiver();
	}

	@Override
	public void streamFrom(StreamProducer<T> upstreamProducer) {
		input.getInput().streamFrom(upstreamProducer);
	}

	@Override
	public void onProducerEndOfStream() {
		input.getInput().onProducerEndOfStream();
	}

	@Override
	public void onProducerError(Exception e) {
		input.getInput().onProducerError(e);
	}

	@Override
	public StreamStatus getConsumerStatus() {
		return input.getInput().getConsumerStatus();
	}

	@Override
	public Exception getConsumerException() {
		return input.getInput().getConsumerException();
	}
}

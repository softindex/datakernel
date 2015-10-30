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
public class StreamProducerDecorator<T> implements StreamProducer<T> {

	private HasOutput<T> output;

	public StreamProducerDecorator() {
	}

	public StreamProducerDecorator(HasOutput<T> output) {
		this.output = output;
	}

	public void setInput(HasOutput<T> output) {
		this.output = output;
	}

	@Override
	public void streamTo(StreamConsumer<T> downstreamConsumer) {
		output.getOutput().streamTo(downstreamConsumer);
	}

	@Override
	public void bindDataReceiver() {
		output.getOutput().bindDataReceiver();
	}

	@Override
	public void onConsumerSuspended() {
		output.getOutput().onConsumerSuspended();
	}

	@Override
	public void onConsumerResumed() {
		output.getOutput().onConsumerResumed();
	}

	@Override
	public void onConsumerError(Exception e) {
		output.getOutput().onConsumerError(e);
	}

	@Override
	public StreamStatus getProducerStatus() {
		return output.getOutput().getProducerStatus();
	}

	@Override
	public Exception getProducerException() {
		return output.getOutput().getProducerException();
	}
}
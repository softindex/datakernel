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
 * It represents object for asynchronous sending streams of data.
 * Implementors of this interface are strongly encouraged to extend one of the abstract classes
 * in this package which implement this interface and make the threading and state management
 * easier.
 *
 * @param <T> type of output data
 */
public interface StreamProducer<T> {
	/**
	 * Changes consumer for this producer, removes itself from previous consumer and removes
	 * previous producer for new consumer. Begins to stream to consumer.
	 *
	 * @param downstreamConsumer consumer for streaming
	 */
	void streamTo(StreamConsumer<T> downstreamConsumer);

	/**
	 * This method is called if consumer was changed for changing consumer status of this producer
	 * and its dependencies
	 */
	void bindDataReceiver();

	/**
	 * This method is called for stop streaming of this producer
	 */
	void onConsumerSuspended();

	/**
	 * This method is called for restore streaming of this producer
	 */
	void onConsumerResumed();

	/**
	 * This method is called for close with error
	 *
	 * @param e exception which was found
	 */
	void onConsumerError(Exception e);

	StreamStatus getProducerStatus();

	Exception getProducerException();
}

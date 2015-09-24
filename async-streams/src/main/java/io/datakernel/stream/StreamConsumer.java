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

import io.datakernel.async.CompletionCallback;

/**
 * It represents an object which can asynchronous receive streams of data.
 * Implementors of this interface are strongly encouraged to extend one of the abstract classes
 * in this package which implement this interface and make the threading and state management
 * easier.
 *
 * @param <T> type of input data
 */
public interface StreamConsumer<T> {
	/**
	 * Returns StreamDataCallback that will process receiving data.
	 * <p>Stream consumer is free to use any appropriate instance implementing the receiver interface, including itself.
	 * <p>Moreover, it is possible (and encouraged) to forward data receiver from 'downstream' consumers.
	 * This design principle makes it possible to implement zero-overhead stream transformers:
	 * <ul>
	 * <li>function transformer with identity function
	 * <li>fan-out stream splitters and sharders with single output
	 * <li>to delegate stream processing to 'downstream' consumers, sequentially one after another
	 * </ul>
	 */
	StreamDataReceiver<T> getDataReceiver();

	/**
	 * Sets wired producer. It will sent data to this consumer
	 *
	 * @param upstreamProducer stream producer for setting
	 */
	void streamFrom(StreamProducer<T> upstreamProducer);

	/**
	 * This method is called when consumer has finished with sending information
	 */
	void onProducerEndOfStream();

	/**
	 * This method is called when consumer has error
	 *
	 * @param e exception which was found
	 */
	void onProducerError(Exception e);

	/**
	 * Adds new CompletionCallback which will be called when consumer closed or closed with error
	 *
	 * @param completionCallback new instance of CompletionCallback
	 */
	void addConsumerCompletionCallback(CompletionCallback completionCallback);
}

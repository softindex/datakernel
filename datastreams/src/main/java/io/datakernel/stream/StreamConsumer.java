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
 * It represents an object which can asynchronous receive streams of data.
 * Implementors of this interface are strongly encouraged to extend one of the abstract classes
 * in this package which implement this interface and make the threading and state management
 * easier.
 *
 * @param <T> type of input data
 */
public interface StreamConsumer<T> {
	/**
	 * Sets wired producer. It will sent data to this consumer
	 *
	 * @param producer stream producer for setting
	 */
	void streamFrom(StreamProducer<T> producer);

	/**
	 * This method is called when consumer has finished with sending information
	 */
	void endOfStream();

	/**
	 * This method is called when consumer has error
	 *
	 * @param e exception which was found
	 */
	void closeWithError(Exception e);
}

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

package io.datakernel.async;

/**
 * It is analogue Iterator from java.util which provides asynchronous iterating of elements.
 *
 * @param <T> the type of elements returned by this iterator
 */
public interface AsyncIterator<T> {
	/**
	 * Non-blocking method. Calls suitable method from IteratorCallback with the next element in the
	 * iteration.
	 */
	void next(IteratorCallback<T> callback);
}



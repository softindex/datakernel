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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestLoggingResultCallback<T> implements ResultCallback<T> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	int results = 0;
	int exceptions = 0;
	T lastResult;
	Exception lastException;

	@Override
	public void onResult(T result) {
		++results;
		lastResult = result;
		logger.info("Got result: {}. Total number of results: {}", result.toString(), results);
	}

	@Override
	public void onException(Exception exception) {
		++exceptions;
		lastException = exception;
		logger.info("Got exception: {}. Total number of exceptions: {}", exception.toString(), exceptions);
	}
}

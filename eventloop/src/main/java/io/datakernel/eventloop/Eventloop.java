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

package io.datakernel.eventloop;

import io.datakernel.time.CurrentTimeProvider;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * An event loop receives tasks  and processes all tasks in the event thread.
 * It will start an exclusive event thread to process all events.
 */
public interface Eventloop extends CurrentTimeProvider {
	/**
	 * Returns the number of loop executions since instantiating Eventloop
	 */
	long tick();

	/**
	 * Adds the event to the beginning of queue from Eventloop's thread
	 *
	 * @param runnable runnable for event
	 */
	void post(Runnable runnable);

	/**
	 * Adds the event to the end of queue from Eventloop's thread
	 *
	 * @param runnable runnable for event
	 */
	void postLater(Runnable runnable);

	/**
	 * Adds the event from another thread
	 *
	 * @param runnable runnable for event
	 */
	void postConcurrently(Runnable runnable);

	/**
	 * Adds {@code callable} from another thread
	 *
	 * @param callable callable
	 * @param <V>      type of result
	 * @return {@link Future} which can be used to retrieve result
	 */
	<V> Future<V> postConcurrently(final Callable<V> callable);

	/**
	 * Adds the event which will be executed after certain time
	 *
	 * @param timestamp time after which this task will be executed
	 * @param runnable  runnable for event
	 * @return {@link ScheduledRunnable} with runnable from argument
	 */
	ScheduledRunnable schedule(long timestamp, Runnable runnable);

	/**
	 * Interface for reporting to Eventloop about the end of concurrent operation
	 */
	interface ConcurrentOperationTracker {
		void complete();
	}

	<T> Object get(Class<T> type);

	<T> void set(Class<T> type, T value);

	ScheduledRunnable scheduleBackground(long timestamp, Runnable runnable);

	/**
	 * It does not let eventloop to stop until all concurrent operations are ended.
	 *
	 * @return {@link ConcurrentOperationTracker} with method Complete, witch can resume
	 * eventloop's work.
	 */
	ConcurrentOperationTracker startConcurrentOperation();
}

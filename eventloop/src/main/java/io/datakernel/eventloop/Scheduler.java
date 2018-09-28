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

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.time.CurrentTimeProvider;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

public interface Scheduler extends CurrentTimeProvider {

	ScheduledRunnable schedule(long timestamp, Runnable runnable);

	default ScheduledRunnable schedule(Instant instant, Runnable runnable) {
		return schedule(instant.toEpochMilli(), runnable);
	}

	default ScheduledRunnable delay(long delayMillis, Runnable runnable) {
		return schedule(currentTimeMillis() + delayMillis, runnable);
	}

	default ScheduledRunnable delay(Duration delay, Runnable runnable) {
		return delay(delay.toMillis(), runnable);
	}

	default <T> Stage<T> schedule(long timestamp, T value) {
		SettableStage<T> result = new SettableStage<>();
		schedule(timestamp, () -> result.set(value));
		return result;
	}

	default <T> Stage<T> schedule(long timestamp, Supplier<T> supplier) {
		SettableStage<T> result = new SettableStage<>();
		schedule(timestamp, () -> result.set(supplier.get()));
		return result;
	}

	default <T> Stage<T> schedule(Instant instant, T value) {
		return schedule(instant.toEpochMilli(), value);
	}

	default <T> Stage<T> schedule(Instant instant, Supplier<T> supplier) {
		return schedule(instant.toEpochMilli(), supplier);
	}

	default <T> Stage<T> delay(long delayMillis, T value) {
		SettableStage<T> result = new SettableStage<>();
		delay(delayMillis, () -> result.set(value));
		return result;
	}

	default <T> Stage<T> delay(long delayMillis, Supplier<T> supplier) {
		SettableStage<T> result = new SettableStage<>();
		delay(delayMillis, () -> result.set(supplier.get()));
		return result;
	}

	default <T> Stage<T> delay(Duration delay, T value) {
		return delay(delay.toMillis(), value);
	}

	default <T> Stage<T> delay(Duration delay, Supplier<T> supplier) {
		return delay(delay.toMillis(), supplier);
	}

	ScheduledRunnable scheduleBackground(long timestamp, Runnable runnable);

	default ScheduledRunnable scheduleBackground(Instant instant, Runnable runnable) {
		return scheduleBackground(instant.toEpochMilli(), runnable);
	}

	default ScheduledRunnable delayBackground(long delayMillis, Runnable runnable) {
		return scheduleBackground(currentTimeMillis() + delayMillis, runnable);
	}

	default ScheduledRunnable delayBackground(Duration delay, Runnable runnable) {
		return delayBackground(delay.toMillis(), runnable);
	}

}

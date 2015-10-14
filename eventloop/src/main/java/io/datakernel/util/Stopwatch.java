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

package io.datakernel.util;

import java.util.concurrent.TimeUnit;

import static io.datakernel.util.Preconditions.check;

public class Stopwatch {
	private boolean isRunning;
	private long start;
	private long nanos;

	public static Stopwatch createUnstarted() { return new Stopwatch(); }

	public static Stopwatch createStarted() {return new Stopwatch().start(); }

	public Stopwatch start() {
		check(!isRunning, "This stopwatch is already running.");
		isRunning = true;
		start = System.nanoTime();
		return this;
	}

	public Stopwatch stop() {
		long tick = System.nanoTime();
		check(isRunning, "This stopwatch is already stopped.");
		isRunning = false;
		nanos += tick - start;
		return this;
	}

	public Stopwatch reset() {
		isRunning = false;
		nanos = 0;
		return this;
	}

	public long time() {
		if (isRunning) {
			return System.nanoTime() - start + nanos;
		} else {
			return nanos;
		}
	}

	public long elapsed(TimeUnit timeUnit) {
		return timeUnit.convert(time(), TimeUnit.NANOSECONDS);
	}
}

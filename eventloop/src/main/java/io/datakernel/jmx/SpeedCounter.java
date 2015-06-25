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

package io.datakernel.jmx;

public final class SpeedCounter {
	private final long periodMillisec;
	private int startValue;
	private long startTime;
	private int currentValue;

	public SpeedCounter(long periodMillisec) {
		this.periodMillisec = periodMillisec;
	}

	public SpeedCounter() {
		this(1000);
	}

	public void add(int value) {
		currentValue += value;
	}

	public void increment() {
		currentValue++;
	}

	public int getSpeedAndReset() {
		long currentTime = System.currentTimeMillis();
		int diffValue = currentValue - startValue;
		long diffTime = currentTime - startTime;
		startValue = currentValue = 0;
		startTime = currentTime;
		if (diffTime == 0)
			return 0;
		return (int) (diffValue * periodMillisec / diffTime);
	}

	public void reset() {
		startValue = currentValue = 0;
		startTime = 0;
	}
}

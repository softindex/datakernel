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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.bitCount;
import static java.lang.Integer.numberOfTrailingZeros;

// Single-threaded
public final class DynamicStatsCounter {
	private final int log2Period;
	private long dynamicMax = Long.MIN_VALUE;
	private long dynamicMin = Long.MAX_VALUE;
	private long dynamicAvg = 0;
	private long dynamicStdDeviation = 0;
	private long dynamicAbsDeviation = 0;
	private int lastValue;

	public DynamicStatsCounter(int period) {
		checkArgument(bitCount(period) == 1, "Period must be power of two");
		this.log2Period = numberOfTrailingZeros(period);
	}

	private long weightLong(long v) {
		return v - (v >> log2Period);
	}

	public void add(int value) {
		lastValue = value;
		long lvalue = value;
		lvalue = lvalue << 32;

		if (dynamicMin == Long.MAX_VALUE) {
			assert dynamicMax == Long.MIN_VALUE;
			dynamicMin = lvalue;
			dynamicMax = lvalue;
		} else {
			long delta = dynamicMax - dynamicMin;
			delta = delta >> log2Period;

			dynamicMax -= delta;
			if (dynamicMax < lvalue)
				dynamicMax = lvalue;

			dynamicMin += delta;
			if (dynamicMin > lvalue)
				dynamicMin = lvalue;
		}

		dynamicAvg = weightLong(dynamicAvg) + lvalue;

		long delta = lvalue - (dynamicAvg >> log2Period);
		dynamicAbsDeviation = weightLong(dynamicAbsDeviation) + Math.abs(delta);
		delta >>= 16;
		dynamicStdDeviation = weightLong(dynamicStdDeviation) + delta * delta;
	}

	public int getLastValue() {
		return lastValue;
	}

	public int getDynamicMax() {
		if (dynamicMax == Long.MIN_VALUE)
			return 0;
		return (int) (dynamicMax >> 32);
	}

	public int getDynamicMin() {
		if (dynamicMin == Long.MAX_VALUE)
			return 0;
		return (int) (dynamicMin >> 32);
	}

	public double getDynamicAvg() {
		return ((double) dynamicAvg) / (1L << (32 + log2Period));
	}

	public double getDynamicStdDeviation() {
		return Math.sqrt(((double) dynamicStdDeviation) / (1L << (32 + log2Period)));
	}

	public double getDynamicAbsDeviation() {
		return (((double) dynamicAbsDeviation) / (1L << (32 + log2Period)));
	}

	public void reset() {
		dynamicMax = Long.MIN_VALUE;
		dynamicMin = Long.MAX_VALUE;
		dynamicAvg = dynamicStdDeviation = dynamicAbsDeviation = 0L;
		lastValue = 0;
	}

	@Override
	public String toString() {
		return String.format("last: %d;  dynamic{min: %d; max: %d; avg: %.2f std: ±%.3f abs: ±%.3f} of %d", getLastValue(), getDynamicMin(), getDynamicMax(),
				getDynamicAvg(), getDynamicStdDeviation(), getDynamicAbsDeviation(), 1L << log2Period);
	}
}

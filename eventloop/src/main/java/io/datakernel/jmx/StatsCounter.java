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

public final class StatsCounter {
	private int min = Integer.MAX_VALUE;
	private int max = Integer.MIN_VALUE;
	private int last;
	private int count;
	private long sum;
	private long sumSqr;

	public void reset() {
		min = Integer.MAX_VALUE;
		max = Integer.MIN_VALUE;
		count = 0;
		last = 0;
		sumSqr = 0;
		sum = 0;
	}

	public void add(int value) {
		setValue(value);
		sum += value;
		sumSqr += sqr(value);
		++count;
	}

	public void incCount() {
		++count;
	}

	public void update(int value, int prevValue) {
		if (value == prevValue)
			return;
		setValue(value);
		sum = sum - prevValue + value;
		sumSqr = sumSqr - sqr(prevValue) + sqr(value);
	}

	private void setValue(int value) {
		last = value;
		if (value < min)
			min = value;
		if (value > max)
			max = value;
	}

	public int getLast() {
		return last;
	}

	public int getMin() {
		return min;
	}

	public int getMax() {
		return max;
	}

	public double getAvg() {
		if (count == 0)
			return 0;
		return (double) sum / count;
	}

	public long getTotal() {
		return sum;
	}

	public double getStd() {
		if (count <= 1)
			return 0;
		double avg = getAvg();
		return Math.sqrt((double) sumSqr / count - avg * avg);
	}

	private static long sqr(int v) {
		return (long) v * v;
	}

	@Override
	public String toString() {
		if (count == 0)
			return "";
		return String.format("%.2f±%.3f min: %d max: %d", getAvg(), getStd(), getMin(), getMax());
	}
}

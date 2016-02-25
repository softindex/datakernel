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

package io.datakernel.aggregation_db.fieldtype;

import static java.lang.Math.exp;
import static java.lang.Math.log;

public final class HyperLogLog implements Comparable<HyperLogLog> {
	private final byte[] registers;

	public HyperLogLog(int registers) {
		this.registers = new byte[registers];
	}

	public HyperLogLog(byte[] registers) {
		this.registers = registers;
	}

	public byte[] getRegisters() {
		return registers;
	}

	public static HyperLogLog union(HyperLogLog a, HyperLogLog b) {
		assert a.registers.length == b.registers.length;
		byte[] buckets = new byte[a.registers.length];
		for (int i = 0; i < a.registers.length; i++) {
			buckets[i] = a.registers[i] > b.registers[i] ? a.registers[i] : b.registers[i];
		}
		return new HyperLogLog(buckets);
	}

	public void union(HyperLogLog another) {
		assert this.registers.length == another.registers.length;
		for (int i = 0; i < this.registers.length; i++) {
			byte thisValue = this.registers[i];
			byte thatValue = another.registers[i];
			if (thatValue > thisValue) {
				this.registers[i] = thatValue;
			}
		}
	}

	public void addToRegister(int register, int valueHash) {
		int zeros = Integer.numberOfTrailingZeros(valueHash) + 1;
		if (this.registers[register] < zeros) {
			this.registers[register] = (byte) zeros;
		}
	}

	public void addLongHash(long longHash) {
		addToRegister((int) (longHash) & (registers.length - 1), (int) (longHash >>> 32));
	}

	public void addObject(Object item) {
		addInt(item.hashCode());
	}

	private static long rehash(long k) {
		k ^= k >>> 33;
		k *= 0xff51afd7ed558ccdL;
		k ^= k >>> 33;
		k *= 0xc4ceb9fe1a85ec53L;
		k ^= k >>> 33;
		return k;
	}

	public void addLong(long value) {
		addLongHash(rehash(value));
	}

	public void addInt(int value) {
		addLongHash(rehash(value));
	}

	private static final double ALPHA_16 = 0.673 * 16 * 16;
	private static final double ALPHA_32 = 0.697 * 32 * 32;
	private static final double ALPHA_64 = 0.709 * 64 * 64;
	private static final double ALPHA_XX = 0.7213;
	private static final double NLOG2 = -log(2.0);

	public int estimate() {
		final int m = registers.length;
		final double alpha;
		if (m == 16) {
			alpha = ALPHA_16;
		} else if (m == 32) {
			alpha = ALPHA_32;
		} else if (m == 64) {
			alpha = ALPHA_64;
		} else {
			alpha = ALPHA_XX / (1 + 1.079 / m) * m * m;
		}

		double sum = 0;
		for (byte value : registers) {
			sum += exp(value * NLOG2);
		}
		double estimate = alpha / sum;

		if (estimate < 5.0 / 2.0 * m) {
			int zeroCount = 0;
			for (byte bucket : registers) {
				if (bucket == 0)
					zeroCount++;
			}
			if (zeroCount != 0) {
				estimate = m * log((double) m / zeroCount);
			}
		}

		return (int) estimate;
	}

	@Override
	public int compareTo(HyperLogLog that) {
		return Integer.compare(this.estimate(), that.estimate());
	}
}

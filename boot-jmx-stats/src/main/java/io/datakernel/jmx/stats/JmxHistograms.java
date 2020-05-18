package io.datakernel.jmx.stats;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.datakernel.common.Preconditions.checkArgument;
import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Math.abs;
import static java.util.Arrays.binarySearch;

public final class JmxHistograms {
	private static final int[] TABLE_10 = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

	static int integerLogBase10(int v) {
		int t = (32 - numberOfLeadingZeros(v)) * 1233 >>> 12;
		return t - (v < TABLE_10[t] ? 1 : 0);
	}

	public static abstract class AbstractJmxHistogram implements JmxHistogram {
		protected final long[] counters;
		protected final int[] levels;

		protected AbstractJmxHistogram(int[] levels) {
			this.counters = new long[levels.length + 1];
			this.levels = levels;
		}

		protected AbstractJmxHistogram(int[] levels, int counters) {
			this.counters = new long[counters];
			this.levels = levels;
		}

		@Override
		public int[] levels() {
			return levels;
		}

		@Override
		public long[] counts() {
			long[] result = new long[levels.length + 1];
			for (int i = 0; i <= levels.length; i++) {
				result[i] = getResult(i);
			}
			return result;
		}

		protected abstract long getResult(int index);

		@Override
		public void reset() {
			Arrays.fill(counters, 0L);
		}

		@Override
		public void add(@NotNull JmxHistogram another) {
			AbstractJmxHistogram anotherImpl = (AbstractJmxHistogram) another;
			checkArgument(Arrays.equals(anotherImpl.levels, levels) && anotherImpl.counters.length == counters.length, "Histograms mismatch");
			for (int i = 0; i < counters.length; i++) {
				this.counters[i] += anotherImpl.counters[i];
			}
		}
	}

	public static final class Base2 extends AbstractJmxHistogram {
		public Base2() {
			super(POWERS_OF_TWO);
		}

		@Override
		public JmxHistogram createAccumulator() {
			return new Base2();
		}

		@Override
		public void record(int value) {
			counters[32 - numberOfLeadingZeros(value)]++;
		}

		@Override
		protected long getResult(int index) {
			return counters[(index + 33 - 1) % 33];
		}
	}

	public static class Base10 extends AbstractJmxHistogram {
		public Base10() {
			super(POWERS_OF_TEN);
		}

		@Override
		public JmxHistogram createAccumulator() {
			return new Base10();
		}

		@Override
		public void record(int value) {
			if (value >= 0) {
				counters[integerLogBase10(value) + 2]++;
			} else {
				counters[0]++;
			}
		}

		@Override
		protected long getResult(int index) {
			return counters[index];
		}
	}

	public static final class Base10Linear extends AbstractJmxHistogram {
		public Base10Linear() {
			super(POWERS_OF_TEN_LINEAR);
		}

		@Override
		public JmxHistogram createAccumulator() {
			return new Base10Linear();
		}

		@Override
		public void record(int value) {
			if (value == 0) {
				counters[1]++;
			} else if (value > 0) {
				int index = (32 - numberOfLeadingZeros(value)) * 1233 >>> 12;
				int power = TABLE_10[index];
				if (value < power) {
					power = TABLE_10[--index];
				}
				int subindex = value / power;
				counters[index * 9 + subindex + 1]++;
			} else if (value < 0) {
				counters[0]++;
			}
		}

		@Override
		protected long getResult(int index) {
			return counters[index];
		}
	}

	public static final class Custom extends AbstractJmxHistogram {
		public Custom(int[] levels) {
			super(levels);
		}

		@Override
		public JmxHistogram createAccumulator() {
			return new Custom(levels);
		}

		@Override
		public void record(int value) {
			counters[abs(binarySearch(levels, value) + 1)]++;
		}

		@Override
		protected long getResult(int index) {
			return counters[index];
		}
	}

}

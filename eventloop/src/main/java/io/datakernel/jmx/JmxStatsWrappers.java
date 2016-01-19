package io.datakernel.jmx;

import javax.management.openmbean.SimpleType;
import java.util.SortedMap;
import java.util.TreeMap;

public final class JmxStatsWrappers {

	private JmxStatsWrappers() {}

	public static JmxSummableValueStats forSummableValue(long value) {
		return new JmxSummableValueStats(value);
	}

	public static JmxMaxValueStats forMaxValue(long value) {
		return new JmxMaxValueStats(value);
	}

	public static JmxMinValueStats forMinValue(long value) {
		return new JmxMinValueStats(value);
	}

	public static JmxAverageValueStats forAverageValue(long value) {
		return new JmxAverageValueStats(value);
	}

	public static JmxLongValueStats forLongValue(long value) {
		return new JmxLongValueStats(value);
	}

	public static JmxDoubleValueStats forDoubleValue(double value) {
		return new JmxDoubleValueStats(value);
	}

	public static JmxStringValueStats forStringValue(String value) {
		return new JmxStringValueStats(value);
	}

	public static final class JmxSummableValueStats implements JmxStats<JmxSummableValueStats> {
		private long sum;

		public JmxSummableValueStats() {
			this.sum = 0L;
		}

		public JmxSummableValueStats(long sum) {
			this.sum = sum;
		}

		@Override
		public void add(JmxSummableValueStats other) {
			this.sum += other.sum;
		}

		@Override
		public void refreshStats(long timestamp, double smoothingWindow) {
			this.sum = 0;
		}

		@Override
		public SortedMap<String, TypeAndValue> getAttributes() {
			SortedMap<String, TypeAndValue> attributes = new TreeMap<>();
			attributes.put("sum", new TypeAndValue(SimpleType.LONG, sum));
			return attributes;
		}
	}

	public static final class JmxMaxValueStats implements JmxStats<JmxMaxValueStats> {
		private long max;

		public JmxMaxValueStats() {
			this.max = Long.MIN_VALUE;
		}

		public JmxMaxValueStats(long max) {
			this.max = max;
		}

		@Override
		public void add(JmxMaxValueStats other) {
			if (other.max > this.max) {
				this.max = other.max;
			}
		}

		@Override
		public void refreshStats(long timestamp, double smoothingWindow) {
			this.max = Long.MIN_VALUE;
		}

		@Override
		public SortedMap<String, TypeAndValue> getAttributes() {
			SortedMap<String, TypeAndValue> attributes = new TreeMap<>();
			attributes.put("max", new TypeAndValue(SimpleType.LONG, max));
			return attributes;
		}
	}

	public static final class JmxMinValueStats implements JmxStats<JmxMinValueStats> {
		private long min;

		public JmxMinValueStats() {
			this.min = Long.MAX_VALUE;
		}

		public JmxMinValueStats(long min) {
			this.min = min;
		}

		@Override
		public void add(JmxMinValueStats other) {
			if (other.min < this.min) {
				this.min = other.min;
			}
		}

		@Override
		public void refreshStats(long timestamp, double smoothingWindow) {
			this.min = Long.MAX_VALUE;
		}

		@Override
		public SortedMap<String, TypeAndValue> getAttributes() {
			SortedMap<String, TypeAndValue> attributes = new TreeMap<>();
			attributes.put("min", new TypeAndValue(SimpleType.LONG, min));
			return attributes;
		}
	}

	public static final class JmxAverageValueStats implements JmxStats<JmxAverageValueStats> {
		private long total;
		private int count;

		public JmxAverageValueStats() {
			this.total = 0L;
			this.count = 0;
		}

		public JmxAverageValueStats(long value) {
			this.total = value;
			this.count = 1;
		}

		@Override
		public void add(JmxAverageValueStats other) {
			this.total += other.total;
			this.count += other.count;
		}

		@Override
		public void refreshStats(long timestamp, double smoothingWindow) {
			this.total = 0L;
			this.count = 0;
		}

		@Override
		public SortedMap<String, TypeAndValue> getAttributes() {
			SortedMap<String, TypeAndValue> attributes = new TreeMap<>();
			double avg = count != 0 ? total / (double) count : 0;
			attributes.put("average", new TypeAndValue(SimpleType.DOUBLE, avg));
			return attributes;
		}
	}

	public static final class JmxLongValueStats implements JmxStats<JmxLongValueStats> {
		private long value;

		public JmxLongValueStats() {
			this.value = 0L;
		}

		public JmxLongValueStats(long value) {
			this.value = value;
		}

		@Override
		public void add(JmxLongValueStats other) {
			this.value = other.value;
		}

		@Override
		public void refreshStats(long timestamp, double smoothingWindow) {
			this.value = 0L;
		}

		@Override
		public SortedMap<String, TypeAndValue> getAttributes() {
			SortedMap<String, TypeAndValue> attributes = new TreeMap<>();
			attributes.put("value", new TypeAndValue(SimpleType.LONG, value));
			return attributes;
		}
	}

	public static final class JmxDoubleValueStats implements JmxStats<JmxDoubleValueStats> {
		private double value;

		public JmxDoubleValueStats() {
			this.value = 0L;
		}

		public JmxDoubleValueStats(double value) {
			this.value = value;
		}

		@Override
		public void add(JmxDoubleValueStats other) {
			this.value = other.value;
		}

		@Override
		public void refreshStats(long timestamp, double smoothingWindow) {
			this.value = 0L;
		}

		@Override
		public SortedMap<String, TypeAndValue> getAttributes() {
			SortedMap<String, TypeAndValue> attributes = new TreeMap<>();
			attributes.put("value", new TypeAndValue(SimpleType.DOUBLE, value));
			return attributes;
		}
	}

	public static final class JmxStringValueStats implements JmxStats<JmxStringValueStats> {
		private String value;

		public JmxStringValueStats() {
			this.value = "";
		}

		public JmxStringValueStats(String value) {
			this.value = value;
		}

		@Override
		public void add(JmxStringValueStats other) {
			this.value = other.value;
		}

		@Override
		public void refreshStats(long timestamp, double smoothingWindow) {
			this.value = "";
		}

		@Override
		public SortedMap<String, TypeAndValue> getAttributes() {
			SortedMap<String, TypeAndValue> attributes = new TreeMap<>();
			attributes.put("value", new TypeAndValue(SimpleType.STRING, value));
			return attributes;
		}
	}
}

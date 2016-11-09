package io.datakernel.cube.api;

import java.util.LinkedHashMap;
import java.util.Map;

public final class Record {
	private final RecordScheme scheme;

	private final Object[] objects;

	private final int[] ints;
	private final double[] doubles;
	private final long[] longs;
	private final float[] floats;

	private Record(RecordScheme scheme) {
		this.scheme = scheme;
		this.objects = scheme.objects != 0 ? new Object[scheme.objects] : null;
		this.ints = scheme.ints != 0 ? new int[scheme.ints] : null;
		this.doubles = scheme.doubles != 0 ? new double[scheme.doubles] : null;
		this.longs = scheme.longs != 0 ? new long[scheme.longs] : null;
		this.floats = scheme.floats != 0 ? new float[scheme.floats] : null;
	}

	public static Record create(RecordScheme scheme) {
		return new Record(scheme);
	}

	public RecordScheme getScheme() {
		return scheme;
	}

	private void putRaw(int rawIndex, Object value) {
		int index = rawIndex & 0xFFFF;
		int rawType = rawIndex >>> 16;
		if (rawType == 1) {
			ints[index] = (int) value;
		} else if (rawType == 2) {
			doubles[index] = (double) value;
		} else if (rawType == 3) {
			longs[index] = (long) value;
		} else if (rawType == 4) {
			floats[index] = (float) value;
		} else if (rawType == 0) {
			objects[index] = value;
		} else {
			throw new IllegalArgumentException();
		}
	}

	private Object getRaw(int rawIndex) {
		int index = rawIndex & 0xFFFF;
		int type = rawIndex >>> 16;
		if (type == 1) {
			return ints[index];
		} else if (type == 2) {
			return doubles[index];
		} else if (type == 3) {
			return longs[index];
		} else if (type == 4) {
			return floats[index];
		} else if (type == 0) {
			return objects[index];
		} else {
			throw new IllegalArgumentException();
		}
	}

	public void put(String field, Object value) {
		putRaw(scheme.fieldRawIndices.get(field), value);
	}

	public void put(int field, Object value) {
		putRaw(scheme.rawIndices[field], value);
	}

	public void putAll(Map<String, Object> values) {
		for (Map.Entry<String, Object> entry : values.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	public void putAll(Object[] values) {
		for (int i = 0; i < values.length; i++) {
			put(i, values[i]);
		}
	}

	public Object get(String field) {
		return getRaw(scheme.fieldRawIndices.get(field));
	}

	public Object get(int field) {
		return getRaw(scheme.rawIndices[field]);
	}

	public Map<String, Object> asMap() {
		Map<String, Object> result = new LinkedHashMap<>(scheme.rawIndices.length * 2);
		getInto(result);
		return result;
	}

	public Object[] asArray() {
		Object[] result = new Object[scheme.rawIndices.length];
		getInto(result);
		return result;
	}

	public void getInto(Map<String, Object> result) {
		for (int i = 0; i < scheme.rawIndices.length; i++) {
			result.put(scheme.fields[i], getRaw(i));
		}
	}

	public void getInto(Object[] result) {
		for (int i = 0; i < scheme.rawIndices.length; i++) {
			result[i] = get(i);
		}
	}

	@Override
	public String toString() {
		return asMap().toString();
	}
}

package io.datakernel.cube.api;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;

import java.util.*;

public final class RecordScheme {
	private final LinkedHashMap<String, Class<?>> fieldTypes = new LinkedHashMap<>();

	protected int objects;
	protected int ints;
	protected int doubles;
	protected int longs;
	protected int floats;

	protected ObjectIntOpenHashMap<String> fieldIndices = new ObjectIntOpenHashMap<>();
	protected ObjectIntOpenHashMap<String> fieldRawIndices = new ObjectIntOpenHashMap<>();
	protected String[] fields = new String[]{};
	protected int[] rawIndices = new int[]{};

	private RecordScheme() {
	}

	public static RecordScheme create() {
		return new RecordScheme();
	}

	public RecordScheme withField(String field, Class<?> type) {
		fieldTypes.put(field, type);
		fields = Arrays.copyOf(fields, fields.length + 1);
		fields[fields.length - 1] = field;
		int rawIndex;
		if (type == Integer.class) {
			rawIndex = (1 << 16) + ints;
			ints++;
		} else if (type == Double.class) {
			rawIndex = (2 << 16) + doubles;
			doubles++;
		} else if (type == Long.class) {
			rawIndex = (3 << 16) + longs;
			longs++;
		} else if (type == Float.class) {
			rawIndex = (4 << 16) + floats;
			floats++;
		} else {
			rawIndex = (0 << 16) + objects;
			objects++;
		}
		fieldIndices.put(field, fieldIndices.size());
		fieldRawIndices.put(field, rawIndex);
		rawIndices = Arrays.copyOf(rawIndices, rawIndices.length + 1);
		rawIndices[rawIndices.length - 1] = rawIndex;
		return this;
	}

	public RecordScheme withFields(Map<String, Class<?>> types) {
		for (String field : types.keySet()) {
			withField(field, types.get(field));
		}
		return this;
	}

	public List<String> getFields() {
		return new ArrayList<>(fieldTypes.keySet());
	}

	public String getField(int index) {
		return fields[index];
	}

	public Class<?> getFieldType(String field) {
		return fieldTypes.get(field);
	}

	public int getFieldIndex(String field) {
		return fieldIndices.get(field);
	}
}

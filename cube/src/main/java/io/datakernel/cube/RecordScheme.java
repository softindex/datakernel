package io.datakernel.cube;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;

import java.lang.reflect.Type;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

public final class RecordScheme {
	private final LinkedHashMap<String, Type> fieldTypes = new LinkedHashMap<>();

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

	public RecordScheme withField(String field, Type type) {
		checkNotNull(type);
		fieldTypes.put(field, type);
		fields = Arrays.copyOf(fields, fields.length + 1);
		fields[fields.length - 1] = field;
		int rawIndex;
		if (type == int.class) {
			rawIndex = (1 << 16) + ints;
			ints++;
		} else if (type == double.class) {
			rawIndex = (2 << 16) + doubles;
			doubles++;
		} else if (type == long.class) {
			rawIndex = (3 << 16) + longs;
			longs++;
		} else if (type == float.class) {
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

	public Type getFieldType(String field) {
		return fieldTypes.get(field);
	}

	public int getFieldIndex(String field) {
		return fieldIndices.get(field);
	}
}

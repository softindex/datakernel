package io.datakernel.jmx;

import javax.management.openmbean.*;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

final class OpenTypeUtils {
	private OpenTypeUtils() {}

	public static SimpleType<?> simpleTypeOf(Class<?> clazz) throws IllegalArgumentException {
		if (clazz == boolean.class || clazz == Boolean.class) {
			return SimpleType.BOOLEAN;
		} else if (clazz == byte.class || clazz == Byte.class) {
			return SimpleType.BYTE;
		} else if (clazz == short.class || clazz == Short.class) {
			return SimpleType.SHORT;
		} else if (clazz == char.class || clazz == Character.class) {
			return SimpleType.CHARACTER;
		} else if (clazz == int.class || clazz == Integer.class) {
			return SimpleType.INTEGER;
		} else if (clazz == long.class || clazz == Long.class) {
			return SimpleType.LONG;
		} else if (clazz == float.class || clazz == Float.class) {
			return SimpleType.FLOAT;
		} else if (clazz == double.class || clazz == Double.class) {
			return SimpleType.DOUBLE;
		} else if (clazz == String.class) {
			return SimpleType.STRING;
		} else {
			throw new IllegalArgumentException("There is no SimpleType for " + clazz.getName());
		}
	}

	public static Class<?> classOf(OpenType<?> openType) {
		if (openType.equals(SimpleType.BOOLEAN)) {
			return boolean.class;
		} else if (openType.equals(SimpleType.BYTE)) {
			return byte.class;
		} else if (openType.equals(SimpleType.SHORT)) {
			return short.class;
		} else if (openType.equals(SimpleType.CHARACTER)) {
			return char.class;
		} else if (openType.equals(SimpleType.INTEGER)) {
			return int.class;
		} else if (openType.equals(SimpleType.LONG)) {
			return long.class;
		} else if (openType.equals(SimpleType.FLOAT)) {
			return float.class;
		} else if (openType.equals(SimpleType.DOUBLE)) {
			return double.class;
		} else if (openType.equals(SimpleType.STRING)) {
			return String.class;
		} else if (openType instanceof CompositeType) {
			return CompositeData.class;
		} else if (openType instanceof TabularType) {
			return TabularData.class;
		}
		// ArrayType is not supported
		throw new IllegalArgumentException(format("OpenType \"%s\" cannot be converted to Class", openType));
	}

	public static Map<String, OpenType<?>> createMapWithOneEntry(String key, OpenType<?> openType) {
		Map<String, OpenType<?>> map = new HashMap<>();
		map.put(key, openType);
		return map;
	}
}

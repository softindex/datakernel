package io.datakernel.util;

public final class ApplicationSettings {
	private ApplicationSettings() {}

	public static String getString(Class<?> type, String name, String defValue) {
		String property;
		property = System.getProperty(type.getName() + "." + name);
		if (property != null) return property;
		property = System.getProperty(type.getSimpleName() + "." + name);
		if (property != null) return property;
		return defValue;
	}

	public static int getInt(Class<?> type, String name, int defValue) {
		String property = getString(type, name, null);
		if (property != null) {
			return Integer.parseInt(property);
		}
		return defValue;
	}

	public static long getLong(Class<?> type, String name, long defValue) {
		String property = getString(type, name, null);
		if (property != null) {
			return Long.parseLong(property);
		}
		return defValue;
	}

	public static boolean getBoolean(Class<?> type, String name, boolean defValue) {
		String property = getString(type, name, null);
		if (property != null) {
			return Boolean.parseBoolean(property);
		}
		return defValue;
	}

	public static double getDouble(Class<?> type, String name, double defValue) {
		String property = getString(type, name, null);
		if (property != null) {
			return Double.parseDouble(property);
		}
		return defValue;
	}
}

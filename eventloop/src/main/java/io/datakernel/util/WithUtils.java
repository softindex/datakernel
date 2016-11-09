package io.datakernel.util;

public class WithUtils {
	private WithUtils() {
	}

	public static <T> T get(WithParameter<T> withParameter) {
		return withParameter.get();
	}
}

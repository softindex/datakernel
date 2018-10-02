package io.datakernel.util;

public final class CharsBuffer {
	private CharsBuffer() {}

	private static final ThreadLocal<char[]> THREAD_LOCAL = ThreadLocal.withInitial(() -> new char[0]);

	public static char[] ensure(int size) {
		char[] chars = THREAD_LOCAL.get();
		if (chars.length >= size) return chars;
		chars = new char[size + (size >>> 2)];
		THREAD_LOCAL.set(chars);
		return chars;
	}
}

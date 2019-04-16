package io.datakernel.aio.nativeio;

import io.datakernel.aio.util.LibraryNativeLoader;

import java.io.IOException;

public final class AioNative {
	static {
		LibraryNativeLoader.loadLibrary("aio_native");
	}

	public static native long io_setup(int maxEvents) throws IOException;

	public static native int io_submit(long context, int n, long aiocb) throws IOException;

	public static native void destroy(long context) throws IOException;

	public static native int io_getevents(long context, long minNumberReturn, long maxNumberReturn,
										  long events, long timeout) throws IOException;
}

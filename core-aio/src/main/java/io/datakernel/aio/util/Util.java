package io.datakernel.aio.util;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

import static io.datakernel.aio.buffer.DirectBufferPool.allocate;
import static io.datakernel.aio.nativeio.NativeOpCodes.IO_CMD_PREAD;
import static io.datakernel.aio.nativeio.NativeOpCodes.IO_CMD_PWRITE;

@SuppressWarnings("Duplicates")
public final class Util {
	private static final int LINK_SIZE = Long.BYTES;
	private static final int CONTROL_BLOCK_SIZE = 1 << 6;
	private static final int SIZE_UNIT = LINK_SIZE + CONTROL_BLOCK_SIZE;

	public static ByteBuffer wrapToNativeReadUnit(ByteBuffer resultBuf, long position, int fd, long callbackId, int size) {
		ByteBuffer nativeUnit = allocate(SIZE_UNIT);

		int offset = LINK_SIZE;
		nativeUnit.putLong(0, getAddress(nativeUnit) + offset);
		nativeUnit.putLong(offset, callbackId);
		nativeUnit.putShort(offset + 16, IO_CMD_PREAD.getCode());
		nativeUnit.putInt(offset + 20, fd);
		nativeUnit.putLong(offset + 24, getAddress(resultBuf));
		nativeUnit.putLong(offset + 32, size);
		nativeUnit.putLong(offset + 40, position);
		return nativeUnit;
	}


	public static ByteBuffer wrapToNativeWriteUnit(int size,
												   long position,
												   Integer fd,
												   long callbackId,
												   ByteBuffer dest) {
		ByteBuffer nativeUnit = allocate(SIZE_UNIT);

		int nativeOffset = LINK_SIZE;
		nativeUnit.putLong(0, getAddress(nativeUnit) + nativeOffset);
		nativeUnit.putLong(nativeOffset, callbackId);
		nativeUnit.putShort(nativeOffset + 16, IO_CMD_PWRITE.getCode());
		nativeUnit.putInt(nativeOffset + 20, fd);
		nativeUnit.putLong(nativeOffset + 24, getAddress(dest));
		nativeUnit.putLong(nativeOffset + 32, size);
		nativeUnit.putLong(nativeOffset + 40, position);
		return nativeUnit;
	}

	public static long getAddress(ByteBuffer buffer) {
		return ((DirectBuffer) buffer).address();
	}
}

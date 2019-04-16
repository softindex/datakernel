package io.datakernel.aio.nativeio;

public enum NativeOpCodes {
	IO_CMD_PREAD,
	IO_CMD_PWRITE;

	public short getCode() {
		return (short) ordinal();
	}
}

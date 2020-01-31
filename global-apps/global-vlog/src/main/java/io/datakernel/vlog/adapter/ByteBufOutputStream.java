package io.datakernel.vlog.adapter;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.common.MemSize;
import org.jetbrains.annotations.NotNull;

import java.io.OutputStream;

public class ByteBufOutputStream extends OutputStream {
	public static final MemSize INITIAL_BUF_SIZE = MemSize.kilobytes(2);

	private final MemSize initialBufSize;
	private ByteBuf byteBuf;
	private ByteBuf collector;

	public ByteBufOutputStream() {
		this(INITIAL_BUF_SIZE);
	}

	public ByteBufOutputStream(MemSize initialSize) {
		this.initialBufSize = initialSize;
		byteBuf = ByteBufPool.allocate(initialSize);
		collector = ByteBufPool.allocate(initialSize);
	}

	@Override
	public void write(int b) {
		if (collector.writeRemaining() > 0) {
			collector.writeByte((byte) b);
		} else {
			this.byteBuf = ByteBufPool.append(byteBuf, collector);
			collector = ByteBufPool.allocate(initialBufSize);
		}
	}

	@Override
	public void write(@NotNull byte[] bytes, int off, int len) {
		this.byteBuf = ByteBufPool.append(byteBuf, bytes, off, len);
	}

	public ByteBuf getBuf() {
		return byteBuf;
	}

	@Override
	public void flush() {
		this.byteBuf = ByteBufPool.append(byteBuf, collector);
	}
}

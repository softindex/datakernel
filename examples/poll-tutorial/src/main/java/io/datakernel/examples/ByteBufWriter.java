package io.datakernel.examples;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import org.jetbrains.annotations.NotNull;

import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

public final class ByteBufWriter extends Writer {
	private ByteBuf byteBuf;
	private final static int DEFAULT_SIZE = 1024;

	public ByteBufWriter() {
		byteBuf = ByteBufPool.allocate(DEFAULT_SIZE);
	}

	@Override
	public void write(@NotNull char[] cbuf, int off, int len) {
		CharBuffer charBuffer = CharBuffer.wrap(cbuf, off, len);
		ByteBuffer buffer = Charset.forName("UTF-8").encode(charBuffer);

		byte[] bytes = Arrays.copyOfRange(buffer.array(),
				buffer.position(), buffer.limit());
		Arrays.fill(buffer.array(), (byte) 0);
		this.byteBuf = ByteBufPool.append(byteBuf, bytes);
	}

	@Override
	public void flush() {
	}

	@Override
	public void close() {
	}

	public ByteBuf getBuf() {
		return byteBuf;
	}
}

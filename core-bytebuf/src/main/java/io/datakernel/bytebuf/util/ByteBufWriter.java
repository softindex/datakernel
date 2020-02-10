package io.datakernel.bytebuf.util;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.common.MemSize;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * An implementation of character {@link Writer}, that appends characters
 * it received to an allocated {@link ByteBuf}.
 * The byte buf can grow (via reallocation) so it is limited only by available RAM.
 * <p>
 * This is a useful proxy between DataKernel {@link ByteBuf ByteBufs} and API's that use standard Java {@link Writer writers}.
 */
public final class ByteBufWriter extends Writer {
	public static final MemSize INITIAL_BUF_SIZE = MemSize.kilobytes(2);

	private final Charset charset;
	private ByteBuf byteBuf;

	private char[] surrogateBuffer;

	public ByteBufWriter() {
		this(INITIAL_BUF_SIZE, StandardCharsets.UTF_8);
	}

	public ByteBufWriter(MemSize initialSize) {
		this(initialSize, StandardCharsets.UTF_8);
	}

	public ByteBufWriter(Charset charset) {
		this(INITIAL_BUF_SIZE, charset);
	}

	public ByteBufWriter(MemSize initialSize, Charset charset) {
		byteBuf = ByteBufPool.allocate(initialSize);
		this.charset = charset;
	}

	@Override
	public void write(@NotNull char[] cbuf, int off, int len) {
		ByteBuffer buffer = charset.encode(CharBuffer.wrap(cbuf, off, len));
		this.byteBuf = ByteBufPool.append(byteBuf, buffer.array(), buffer.position(), buffer.limit());
	}

	// Override all writer methods without IOException since we never throw it

	@Override
	public void write(int c) {
		// sneaky little buffering for proper encoding of surrogate character pairs
		synchronized (lock) {
			if (surrogateBuffer == null) {
				surrogateBuffer = new char[2];
			}
			char ch = (char) c;
			if (Character.isLowSurrogate(ch)) {
				surrogateBuffer[1] = ch;
				write(surrogateBuffer, 0, 2);
			} else {
				surrogateBuffer[0] = ch;
				if (!Character.isHighSurrogate(ch)) {
					write(surrogateBuffer, 0, 1);
				}
			}
		}
	}

	@Override
	public void write(@NotNull char[] cbuf) {
		try {
			super.write(cbuf);
		} catch (IOException ignored) {
			throw new AssertionError("unreachable");
		}
	}

	@Override
	public void write(@NotNull String str) {
		try {
			super.write(str);
		} catch (IOException ignored) {
			throw new AssertionError("unreachable");
		}
	}

	@Override
	public void write(@NotNull String str, int off, int len) {
		try {
			super.write(str, off, len);
		} catch (IOException ignored) {
			throw new AssertionError("unreachable");
		}
	}

	@Override
	public Writer append(CharSequence csq) {
		try {
			return super.append(csq);
		} catch (IOException ignored) {
			throw new AssertionError("unreachable");
		}
	}

	@Override
	public Writer append(CharSequence csq, int start, int end) {
		try {
			return super.append(csq, start, end);
		} catch (IOException ignored) {
			throw new AssertionError("unreachable");
		}
	}

	@Override
	public Writer append(char c) {
		try {
			return super.append(c);
		} catch (IOException ignored) {
			throw new AssertionError("unreachable");
		}
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

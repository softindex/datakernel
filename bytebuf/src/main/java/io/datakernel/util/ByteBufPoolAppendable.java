package io.datakernel.util;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;

import static io.datakernel.bytebuf.ByteBufStrings.putUtf8;

public final class ByteBufPoolAppendable implements Appendable {
	public static final MemSize INITIAL_BUF_SIZE = MemSize.kilobytes(2);

	ByteBuf container;

	public ByteBufPoolAppendable() {
		this(INITIAL_BUF_SIZE);
	}

	ByteBufPoolAppendable(MemSize size) {
		this.container = ByteBufPool.allocate(size);
	}

	ByteBufPoolAppendable(int size) {
		this.container = ByteBufPool.allocate(size);
	}

	@Override
	public Appendable append(CharSequence csq) {
		container = ByteBufPool.ensureWriteRemaining(container, csq.length() * 3);
		for (int i = 0; i < csq.length(); i++) {
			putUtf8(container, csq.charAt(i));
		}
		return this;
	}

	@Override
	public Appendable append(CharSequence csq, int start, int end) {
		return append(csq.subSequence(start, end));
	}

	@Override
	public Appendable append(char c) {
		container = ByteBufPool.ensureWriteRemaining(container, 3);
		putUtf8(container, c);
		return this;
	}

	public ByteBuf get() {
		return container;
	}
}

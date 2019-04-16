package io.datakernel.aio.buffer;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

final class LinkedDirectBuffer {
	final ByteBuffer buffer;
	LinkedDirectBuffer next;

	public LinkedDirectBuffer(ByteBuffer buffer) {
		if (!(buffer instanceof DirectBuffer)) {
			throw new IllegalArgumentException("Cannot accept the heap buffer");
		}

		this.buffer = buffer;
	}
}

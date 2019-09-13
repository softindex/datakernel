package io.global.comm.util;

import io.datakernel.codec.StructuredEncoder;
import io.datakernel.codec.StructuredOutput;

public class LazyEncoder<T> implements StructuredEncoder<T> {
	private StructuredEncoder<T> peer = null;

	public void realize(StructuredEncoder<T> peer) {
		this.peer = peer;
	}

	@Override
	public void encode(StructuredOutput out, T item) {
		peer.encode(out, item);
	}
}

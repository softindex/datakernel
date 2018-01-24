package io.datakernel.stream.stats;

import io.datakernel.bytebuf.ByteBuf;

import java.util.Collection;

public interface StreamStatsSizeCounter<T> {
	int size(T item);

	static StreamStatsSizeCounter<ByteBuf> forByteBufs() {
		return ByteBuf::readRemaining;
	}

	static <T extends Collection<?>> StreamStatsSizeCounter<T> forCollections() {
		return Collection::size;
	}

	static <T> StreamStatsSizeCounter<T[]> forArrays() {
		return item -> item.length;
	}

	static StreamStatsSizeCounter<String> forStrings() {
		return String::length;
	}
}

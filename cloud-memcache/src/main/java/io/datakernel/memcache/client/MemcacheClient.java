package io.datakernel.memcache.client;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;

public interface MemcacheClient {
	Promise<Void> put(byte[] key, ByteBuf buf, int timeout);

	Promise<ByteBuf> get(byte[] key, int timeout);

	default Promise<Void> put(byte[] key, ByteBuf buf) {
		return put(key, buf, Integer.MAX_VALUE);
	}

	default Promise<ByteBuf> get(byte[] key) {
		return get(key, Integer.MAX_VALUE);
	}
}

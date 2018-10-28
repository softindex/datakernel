package io.datakernel.loader;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.http.HttpException;
import io.datakernel.loader.cache.Cache;

class CachedStaticLoader implements StaticLoader {
	private static final byte[] BYTES_ERROR = new byte[]{};

	private final StaticLoader resourceLoader;
	private final Cache cache;

	public CachedStaticLoader(StaticLoader resourceLoader, Cache cache) {
		this.resourceLoader = resourceLoader;
		this.cache = cache;
	}

	@Override
	public Promise<ByteBuf> getResource(String name) {
		byte[] bytes = cache.get(name);

		if (bytes == null) {
			return resourceLoader.getResource(name)
					.whenComplete((buf, e) ->
							cache.put(name, e == null ? buf.getArray() : BYTES_ERROR));
		} else if (bytes == BYTES_ERROR) {
			return Promise.ofException(HttpException.notFound404());
		} else {
			return Promise.of(ByteBuf.wrapForReading(bytes));
		}
	}
}

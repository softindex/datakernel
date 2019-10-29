package io.datakernel.http.loader;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.promise.Promise;

import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.datakernel.bytebuf.ByteBuf.wrapForReading;

class StaticLoaderCache implements StaticLoader {
	public static final byte[] NOT_FOUND = {};

	private final StaticLoader resourceLoader;
	private final Function<String, byte[]> get;
	private final BiConsumer<String, byte[]> put;

	public StaticLoaderCache(StaticLoader resourceLoader, Function<String, byte[]> get, BiConsumer<String, byte[]> put) {
		this.resourceLoader = resourceLoader;
		this.get = get;
		this.put = put;
	}

	@Override
	public Promise<ByteBuf> load(String path) {
		byte[] bytes = get.apply(path);
		if (bytes == NOT_FOUND) {
			return Promise.ofException(NOT_FOUND_EXCEPTION);
		} else if (bytes != null) {
			return Promise.of(wrapForReading(bytes));
		} else {
			return doLoad(path);
		}
	}

	private Promise<ByteBuf> doLoad(String path) {
		return resourceLoader.load(path)
				.whenComplete((buf, e2) -> {
					if (e2 == null) {
						put.accept(path, buf.getArray());
					} else if (e2 == NOT_FOUND_EXCEPTION) {
						put.accept(path, NOT_FOUND);
					}
				});
	}
}

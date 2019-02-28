package io.datakernel.loader;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.file.AsyncFile;
import io.datakernel.http.HttpException;

import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.Executor;

class SimpleStaticLoaderAsync implements StaticLoader {
	private final Executor executor;
	private final Path root;

	public SimpleStaticLoaderAsync(Executor executor, Path root) {
		this.executor = executor;
		this.root = root;
	}

	@Override
	public Promise<ByteBuf> getResource(String name) {
		Path file = root.resolve(name).normalize();

		if (!file.startsWith(root)) {
			return Promise.ofException(HttpException.notFound404());
		}

		return AsyncFile.readFile(executor, file)
				.thenComposeEx((buf, e) -> {
					if (e instanceof NoSuchFileException) {
						return Promise.ofException(HttpException.notFound404());
					}
					return Promise.of(buf, e);
				});
	}
}

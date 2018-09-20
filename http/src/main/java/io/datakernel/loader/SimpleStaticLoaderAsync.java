package io.datakernel.loader;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.file.AsyncFile;
import io.datakernel.http.HttpException;

import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

class SimpleStaticLoaderAsync implements StaticLoader {
	private final ExecutorService executorService;
	private final Path root;

	public SimpleStaticLoaderAsync(ExecutorService executorService, Path root) {
		this.executorService = executorService;
		this.root = root;
	}

	@Override
	public Stage<ByteBuf> getResource(String name) {
		Path file = root.resolve(name).normalize();

		if (!file.startsWith(root)) {
			return Stage.ofException(HttpException.notFound404());
		}

		return AsyncFile.readFile(executorService, file)
				.thenComposeEx((buf, e) -> {
					if (e instanceof NoSuchFileException) {
						return Stage.ofException(HttpException.notFound404());
					}
					return Stage.of(buf, e);
				});
	}
}

package io.datakernel.loader;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.file.ChannelFileReader;
import io.datakernel.http.HttpException;

import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.Executor;

class SimpleStaticLoaderAsync implements StaticLoader {
	private final Path root;

	public SimpleStaticLoaderAsync(Path root) {
		this.root = root;
	}

	public static StaticLoader create(Path dir) {
		return new SimpleStaticLoaderAsync(dir);
	}

	@Override
	public Promise<ByteBuf> getResource(String name) {
		Path file = root.resolve(name).normalize();

		if (!file.startsWith(root)) {
			return Promise.ofException(HttpException.notFound404());
		}

		return ChannelFileReader.readFile(file)
				.then(cfr -> cfr.toCollector(ByteBufQueue.collector()))
				.thenEx((buf, e) -> {
					if (e instanceof NoSuchFileException) {
						return Promise.ofException(HttpException.notFound404());
					}
					return Promise.of(buf, e);
				});
	}
}

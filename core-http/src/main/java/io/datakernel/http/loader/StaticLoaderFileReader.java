package io.datakernel.http.loader;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.file.ChannelFileReader;
import io.datakernel.promise.Promise;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executor;

class StaticLoaderFileReader implements StaticLoader {
	private final Executor executor;
	private final Path root;

	private StaticLoaderFileReader(Executor executor, Path root) {
		this.executor = executor;
		this.root = root;
	}

	public static StaticLoader create(Executor executor, Path dir) {
		return new StaticLoaderFileReader(executor, dir);
	}

	@Override
	public Promise<ByteBuf> load(String path) {
		Path file = root.resolve(path).normalize();

		if (!file.startsWith(root)) {
			return Promise.ofException(NOT_FOUND_EXCEPTION);
		}

		return Promise.ofBlockingCallable(executor,
				() -> {
					if (Files.isRegularFile(file)) {
						return null;
					}
					if (Files.isDirectory(file)) {
						throw IS_A_DIRECTORY;
					} else {
						throw NOT_FOUND_EXCEPTION;
					}
				})
				.then($ -> ChannelFileReader.open(executor, file))
				.then(cfr -> cfr.toCollector(ByteBufQueue.collector()));
	}
}

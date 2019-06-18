package io.datakernel.loader;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.file.ChannelFileReader;

import java.nio.file.Files;
import java.nio.file.Path;

class StaticLoaderFileReader implements StaticLoader {
	private final Path root;

	public StaticLoaderFileReader(Path root) {
		this.root = root;
	}

	public static StaticLoader create(Path dir) {
		return new StaticLoaderFileReader(dir);
	}

	@Override
	public Promise<ByteBuf> load(String path) {
		Path file = root.resolve(path).normalize();

		if (!file.startsWith(root)) {
			return Promise.ofException(NOT_FOUND_EXCEPTION);
		}

		return Promise.ofBlockingCallable(
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
				.then($ -> ChannelFileReader.readFile(file))
				.then(cfr -> cfr.toCollector(ByteBufQueue.collector()));
	}
}

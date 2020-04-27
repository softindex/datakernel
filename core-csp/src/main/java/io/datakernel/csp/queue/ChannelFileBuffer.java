package io.datakernel.csp.queue;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.MemSize;
import io.datakernel.csp.file.ChannelFileReader;
import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executor;

import static java.nio.file.StandardOpenOption.*;

public final class ChannelFileBuffer implements ChannelQueue<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(ChannelFileBuffer.class);

	private final ChannelFileReader reader;
	private final ChannelFileWriter writer;
	private final Executor executor;
	private final Path path;

	@Nullable
	private SettablePromise<ByteBuf> take;

	private boolean finished = false;

	@Nullable
	private Exception exception;

	private ChannelFileBuffer(ChannelFileReader reader, ChannelFileWriter writer, Executor executor, Path path) {
		this.reader = reader;
		this.writer = writer;
		this.executor = executor;
		this.path = path;
	}

	public static Promise<ChannelFileBuffer> create(Executor executor, Path filePath) {
		return create(executor, filePath, null);
	}

	public static Promise<ChannelFileBuffer> create(Executor executor, Path path, @Nullable MemSize limit) {
		return Promise.ofBlockingCallable(executor, () -> {
			Files.createDirectories(path.getParent());
			ChannelFileWriter writer = ChannelFileWriter.openBlocking(executor, path, CREATE, WRITE);
			ChannelFileReader reader = ChannelFileReader.openBlocking(executor, path, CREATE, READ);
			if (limit != null) {
				reader.withLength(limit.toLong());
			}
			return new ChannelFileBuffer(reader, writer, executor, path);
		});
	}

	@Override
	public Promise<Void> put(@Nullable ByteBuf item) {
		if (exception != null) {
			return Promise.ofException(exception);
		}
		if (item == null) {
			finished = true;
		}
		if (take == null) {
			return writer.accept(item);
		}
		SettablePromise<ByteBuf> promise = take;
		take = null;
		promise.set(item);
		return item == null ?
				writer.accept(null) :
				Promise.complete();
	}

	@Override
	public Promise<ByteBuf> take() {
		if (exception != null) {
			return Promise.ofException(exception);
		}
		if (!isExhausted()) {
			return reader.get();
		}
		if (finished) {
			return Promise.of(null);
		}
		SettablePromise<ByteBuf> promise = new SettablePromise<>();
		take = promise;
		return promise;
	}

	@Override
	public boolean isSaturated() {
		return false;
	}

	@Override
	public boolean isExhausted() {
		return reader.getPosition() >= writer.getPosition();
	}

	@Override
	public void closeEx(@NotNull Throwable e) {
		if (exception != null) {
			return;
		}
		exception = e instanceof Exception ? (Exception) e : new RuntimeException(e);
		writer.closeEx(e);
		reader.closeEx(e);

		if (take != null) {
			take.setException(e);
			take = null;
		}

		// each queue should operate on files with unique names
		// to avoid races due to this
		executor.execute(() -> {
			try {
				Files.deleteIfExists(path);
			} catch (IOException io) {
				logger.error("failed to cleanup channel buffer file " + path, io);
			}
		});
	}

	@Nullable
	public Exception getException() {
		return exception;
	}
}

package io.datakernel.async.file;

import io.datakernel.common.Preconditions;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.promise.Promise;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Executor;

import static io.datakernel.promise.Promise.ofBlockingCallable;

public final class ExecutorAsyncFileService implements AsyncFileService {
	private final Executor executor;

	public ExecutorAsyncFileService(Executor executor) {
		this.executor = Preconditions.checkNotNull(executor);
	}

	@Override
	public Promise<Integer> read(FileChannel channel, long position, byte[] array, int offset, int size) {
		return ofBlockingCallable(executor, () -> {
			ByteBuffer buffer = ByteBuffer.wrap(array, offset, size);
			long pos = position;

			do {
				try {
					int readBytes = channel.read(buffer, pos);
					if (readBytes == -1) {
						break;
					}
					pos += readBytes;
				} catch (IOException e) {
					throw new UncheckedException(e);
				}
			} while (buffer.position() < buffer.limit());
			return Math.toIntExact(pos - position);
		});
	}

	@Override
	public Promise<Integer> write(FileChannel channel, long position, byte[] array, int offset, int size) {
		return ofBlockingCallable(executor, () -> {
			ByteBuffer buffer = ByteBuffer.wrap(array, offset, size);
			long pos = position;

			try {
				do {
					pos += channel.write(buffer, pos);
				} while (buffer.position() < buffer.limit());
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
			return Math.toIntExact(pos - position);
		});
	}
}

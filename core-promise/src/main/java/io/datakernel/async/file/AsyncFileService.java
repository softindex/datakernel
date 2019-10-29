package io.datakernel.async.file;

import io.datakernel.promise.Promise;

import java.nio.channels.FileChannel;

public interface AsyncFileService {
	Promise<Integer> read(FileChannel channel, long position, byte[] array, int offset, int size);

	Promise<Integer> write(FileChannel channel, long position, byte[] array, int offset, int size);
}

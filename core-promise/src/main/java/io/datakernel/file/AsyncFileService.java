package io.datakernel.file;

import io.datakernel.async.Promise;

import java.nio.channels.FileChannel;

public interface AsyncFileService {
	Promise<Integer> read(FileChannel channel, long position, byte[] array, int offset, int size);

	Promise<Integer> write(FileChannel channel, long position, byte[] array, int offset, int size);
}
